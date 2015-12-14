#!/usr/bin/env python
# awsfile.py
# before upload or download a file:
#     export AWS_ACCESS_KEY_ID='key_id'
#     export AWS_SECRET_ACCESS_KEY='key'
#     export AWS_DEFAULT_REGION='region'
# or
#     aws configure
#


import os
import re
import sys
import boto3
import shutil
import logging
import argparse

from threading import RLock
from collections import namedtuple
from logging.handlers import SysLogHandler
from concurrent.futures import ThreadPoolExecutor
from boto3.exceptions import RetriesExceededError

rlock = RLock()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(SysLogHandler(address='/dev/log'))

MB = 1024*1024
PREFIX_S3 = 's3://'
# s3 largest file size is 5T
# and PartNumber is a positive integer between 1 and 10,000
# https://aws.amazon.com/blogs/aws/amazon-s3-object-size-limit/
# http://boto3.readthedocs.org/en/latest/reference/services/s3.html
# MAX_CHUNK_SIZE is safe setting to 5T / 10000?
MAX_CHUNK_SIZE = 500*MB
MIN_CHUNK_SIZE = 5*MB


def _GET_CHUNK_SIZE(file_size):
    chunk_size = max(MIN_CHUNK_SIZE,
                     min(MAX_CHUNK_SIZE, int(file_size/5000)))
    LOGGER.debug("set chunk_size to %s" % chunk_size)
    return chunk_size


class FileChunkReader(object):
    def __init__(self, filename, callback=None):
        self._filename = filename
        self._callback = callback
        self._file_handler = open(self._filename, 'rb')
        self._file_size = int(os.fstat(self._file_handler.fileno()).st_size)
        self._chunk_size = _GET_CHUNK_SIZE(self._file_size)

        self._chunks = []
        self._gen_chunks()

    def _reader(self, starter, size):
        def read():
            if rlock.acquire():
                self._file_handler.seek(starter)
                data = self._file_handler.read(size)
                rlock.release()
            # TODO, callback here
            return data

        assert((starter / float(self._chunk_size)) % 1 == 0)
        part_number = int(starter/self._chunk_size) + 1

        reader = namedtuple('Reader', 'read part_number size')
        return reader(read=read, part_number=part_number, size=size)

    def _gen_chunks(self):
        parts_num = int(self._file_size / float(self._chunk_size))
        for i in range(0, parts_num):
            starter = self._chunk_size * i
            self._chunks.append(self._reader(starter, self._chunk_size))

        last_starter = self._chunk_size * parts_num
        last_size = self._file_size - last_starter
        self._chunks.append(self._reader(last_starter, last_size))

    def get_chunks(self):
        return self._chunks

    def get_size(self):
        return self._file_size


class FileChunkWriter(object):
    def __init__(self, filename, size, callback=None):
        self._filename = filename
        self._file_size = size
        self._chunk_size = _GET_CHUNK_SIZE(self._file_size)
        self._file_handler = open("%s.download" % self._filename, 'wb')
        # self._file_handler.seek(self._file_size - 1)
        # self._file_handler.write('\0')

        self._chunks = []
        self._gen_chunks()

    def _writer(self, starter, size):
        def write(data):
            assert(size == len(data))
            if rlock.acquire():
                self._file_handler.seek(starter)
                self._file_handler.write(data)
                rlock.release()
            # TODO, callback here

        range_param = 'bytes=%s-%s' % (starter, starter + size - 1)
        writer = namedtuple('Writer', 'write range_param size')

        return writer(write=write, range_param=range_param, size=size)

    def _gen_chunks(self):
        parts_num = int(self._file_size / float(self._chunk_size))
        for i in range(0, parts_num):
            starter = self._chunk_size * i
            self._chunks.append(self._writer(starter, self._chunk_size))

        last_starter = self._chunk_size * parts_num
        last_size = self._file_size - last_starter
        self._chunks.append(self._writer(last_starter, last_size))

    def get_chunks(self):
        return self._chunks

    def commit_write(self):
        shutil.move("%s.download" % self._filename, self._filename)

    def abort_write(self):
        if os.path.exists("%s.download" % self._filename):
            os.remove("%s.download" % self._filename)


class FileTransfer(object):

    def __init__(self, retry=10, progress=False, threads=4, callback=None):

        self._retry = retry
        self._progress = progress
        self._threads = threads
        self._callback = callback

        self._finished_size = 0
        self._total_size = 0
        self._parts_number = 0
        self.client = boto3.client('s3')

    def copy_file(self, src, dest, extra_args={}):
        src_is_s3 = src.startswith(PREFIX_S3)
        dest_is_s3 = dest.startswith(PREFIX_S3)

        if src_is_s3 and (not dest_is_s3):
            bucket, key = self._split_s3uri(src)
            return self._download(bucket, key, dest, extra_args)
        elif (not src_is_s3) and dest_is_s3:
            bucket, key = self._split_s3uri(dest)
            return self._upload(src, bucket, key, extra_args)
        else:
            return False

    def _split_s3uri(self, s3uri):
        m = re.match('^%s(.*?)/(.*)$' % PREFIX_S3, s3uri)
        assert m is not None
        r = m.groups()
        assert len(r) == 2

        return r

    def _download(self, bucket, key, dest, extra_args):
        write_file = "%s/%s" % (dest, key.split('/')[-1]) \
                     if os.path.isdir(dest) else dest
        file_size = int(self.client.head_object(
            Bucket=bucket, Key=key, **extra_args)['ContentLength'])
        self._total_size = file_size
        writer = FileChunkWriter(write_file, file_size)
        chunks = writer.get_chunks()

        executor = ThreadPoolExecutor(max_workers=self._threads)
        do_download = self._download_one_part(bucket, key, extra_args)
        try:
            map(lambda x: x, executor.map(do_download, chunks))
            writer.commit_write()
        except (KeyboardInterrupt, Exception) as error:
            writer.abort_write()
            executor.shutdown()
            LOGGER.error("abort download %s %s %s" % (bucket, key, dest))
            raise(error)

    def _download_one_part(self, bucket, key, extra_args):
        def _do_download(chunk):
            for r in range(0, 10):
                response = self.client.get_object(
                    Bucket=bucket, Key=key,
                    Range=chunk.range_param, **extra_args)
                if response['ResponseMetadata']['HTTPStatusCode'] \
                        is not 206:
                    continue
                data = response['Body'].read()
                assert len(data) == chunk.size
                chunk.write(data)

                self._add_finished_size(chunk.size)
                LOGGER.debug("download %s %s" % (key, chunk.range_param))

                if self._progress:
                    self._callback(self._total_size, self._finished_size)

                return True

            raise(RetriesExceededError("download retries exceeded"))

        return _do_download

    def _upload(self, src, bucket, key, extra_args):
        reader = FileChunkReader(src)
        self._total_size = reader.get_size()
        chunks = reader.get_chunks()
        self._parts_number = len(chunks)

        response = self.client.create_multipart_upload(
            Bucket=bucket, Key=key, **extra_args)
        upload_id = response['UploadId']

        executor = ThreadPoolExecutor(max_workers=self._threads)
        try:
            do_upload = self._upload_one_part(bucket, key,
                                              upload_id, extra_args)

            # multi upload process.
            parts = map(lambda x: x, executor.map(do_upload, chunks))
            self.client.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id,
                MultipartUpload={'Parts': parts})

        except (KeyboardInterrupt, Exception) as error:
            executor.shutdown()
            LOGGER.error("abort upload %s %s %s" % (src, bucket, key))
            self.client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id)
            raise(error)

    def _upload_one_part(self, bucket, key, upload_id, extra_args):
        def _do_upload(chunk):
            for r in range(0, 10):
                response = self.client.upload_part(
                    Bucket=bucket, Key=key,
                    UploadId=upload_id, PartNumber=chunk.part_number,
                    Body=chunk.read(), **extra_args)
                etag = response['ETag']
                if response['ResponseMetadata']['HTTPStatusCode'] \
                        is not 200:
                    continue
                self._add_finished_size(chunk.size)
                LOGGER.debug("upload %s %s of %s" % (
                             key, chunk.part_number, self._parts_number))

                if self._progress:
                    self._callback(self._total_size, self._finished_size)

                return {'ETag': etag, 'PartNumber': chunk.part_number}
            raise(RetriesExceededError("upload retries exceeded"))

        return _do_upload

    def _add_finished_size(self, size):
        if rlock.acquire():
            self._finished_size += size
            rlock.release()


def parse_args():
    parser = argparse.ArgumentParser(
        description='upload and download file for aws s3')

    parser.add_argument(
        '-r', '--retry', type=int, help='retries for each chunk')
    parser.add_argument('-p', '--progress', action='store_true',
                        help='show transfer progress in percentage')
    parser.add_argument(
        '-t', '--threads', type=int, help='number of worker threads')
    parser.add_argument('src')
    parser.add_argument('dest')

    return parser.parse_args()


def upload_callback(total_size, finished_size):
    assert total_size > 0
    sys.stdout.write("%2.4f%%\r" % (finished_size*100/float(total_size)))
    sys.stdout.flush()


if "__main__" == __name__:
    args = parse_args()

    src, dest = args.src, args.dest
    retry = args.retry if args.retry else 10
    progress = args.progress if args.progress else False
    threads = args.threads if args.threads else 5

    transfer = FileTransfer(retry, progress, threads, upload_callback)
    transfer.copy_file(src, dest)
    print
