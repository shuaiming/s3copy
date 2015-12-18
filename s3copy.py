#!/usr/bin/env python
#
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
import time
import boto3
import shutil
import logging
import argparse

from hashlib import md5
from threading import RLock
from collections import namedtuple
from logging.handlers import SysLogHandler
from concurrent.futures import ThreadPoolExecutor
from boto3.exceptions import RetriesExceededError

rlock = RLock()
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
LOGGER.addHandler(SysLogHandler(address='/dev/log'))

PREFIX_S3 = 's3://'

KB = 1024
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB

# s3 largest file size is 5T
# and PartNumber is a positive integer between 1 and 10,000
# https://aws.amazon.com/blogs/aws/amazon-s3-object-size-limit/
# http://boto3.readthedocs.org/en/latest/reference/services/s3.html
# MAX_CHUNK_SIZE is safe setting to 5T / 10000?
# Each part must be at least 5 MB in size, except the last part.
MIN_CHUNK_SIZE = 5 * MB
MAX_CHUNK_SIZE = 500 * MB
MAX_S3_FILE_SIZE = 5 * TB


def _GET_CHUNK_SIZE(file_size):
    """
    +------------+------------+---------------+
    | file_size  | chunk_size | parts         |
    +------------+------------+---------------+
    | < 500M     | file_size  | no Multipart  |
    | 500M - 25G | 5M         | 100 - 5000    |
    | 25G - 250G | 50M        | 500 - 5000    |
    | 250G - 1T  | 125M       | 2000 - 8000   |
    | 1T - 5T    | 500M       | 2000 - 10000  |
    +------------+------------+---------------+
    """
    assert file_size <= 5 * TB
    if file_size <= 500 * MB:
        chunk_size = file_size
    elif file_size <= 25 * GB:
        chunk_size = MIN_CHUNK_SIZE
    elif file_size <= 250 * GB:
        chunk_size = 50 * MB
    elif file_size <= 1 * TB:
        chunk_size = 125 * MB
    else:
        chunk_size = MAX_CHUNK_SIZE

    LOGGER.info("set chunk_size to %s" % chunk_size)
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
            return data

        assert((starter / float(self._chunk_size)) % 1 == 0)
        part_number = int(starter/self._chunk_size) + 1

        reader = namedtuple('Reader', 'read part_number size')
        return reader(read=read, part_number=part_number, size=size)

    def _gen_chunks(self):
        parts_num = int(self._file_size / float(self._chunk_size))
        if self._file_size == self._chunk_size:
            parts_num = 0

        for i in range(0, parts_num):
            starter = self._chunk_size * i
            self._chunks.append(self._reader(starter, self._chunk_size))

        last_starter = self._chunk_size * parts_num
        last_size = self._file_size - last_starter
        self._chunks.append(self._reader(last_starter, last_size))

    def get_chunks(self):
        return self._chunks

    def get_chunk_size(self):
        return self._chunk_size

    def get_size(self):
        return self._file_size


class FileChunkWriter(object):
    def __init__(self, filename, size, callback=None):
        self._filename = filename
        self._file_size = size
        self._chunk_size = _GET_CHUNK_SIZE(self._file_size)
        self._file_handler = open("%s.download" % self._filename, 'wb')
        self._file_handler.seek(self._file_size - 1)
        self._file_handler.write('\0')

        self._chunks = []
        self._gen_chunks()

    def _writer(self, starter, size):
        def write(data):
            assert(size == len(data))
            if rlock.acquire():
                self._file_handler.seek(starter)
                self._file_handler.write(data)
                rlock.release()

        range_param = 'bytes=%s-%s' % (starter, starter + size - 1)
        writer = namedtuple('Writer', 'write range_param size')

        return writer(write=write, range_param=range_param, size=size)

    def _gen_chunks(self):
        parts_num = int(self._file_size / float(self._chunk_size))
        if self._file_size == self._chunk_size:
            parts_num = 0
        for i in range(0, parts_num):
            starter = self._chunk_size * i
            self._chunks.append(self._writer(starter, self._chunk_size))

        last_starter = self._chunk_size * parts_num
        last_size = self._file_size - last_starter
        self._chunks.append(self._writer(last_starter, last_size))

    def get_chunks(self):
        return self._chunks

    def get_chunk_size(self):
        return self._chunk_size

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

    def verify_file(self, src, dest):
        return (self._calculate_etag(src), self._calculate_etag(dest))

    def etag_file(self, dest):
        return self._calculate_etag(dest)

    def _calculate_etag(self, dest):
        etag = None

        def md5_cal(chunk):
            self._add_finished_size(chunk.size)
            if self._progress:
                self._callback(self._total_size, self._finished_size)
            return md5(chunk.read()).digest()

        if dest.startswith(PREFIX_S3):
            bucket, key = self._split_s3uri(dest)
            response = self.client.head_object(Bucket=bucket, Key=key)
            etag = str(response['ETag']).strip('"')
        else:
            reader = FileChunkReader(dest)
            self._total_size = reader.get_size()
            chunks = reader.get_chunks()
            b = reduce(lambda x, y: x+y, map(md5_cal, chunks))
            etag = "%s-%s" % (md5(b).hexdigest(), len(chunks))

        return etag

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
        return True

    def _download_one_part(self, bucket, key, extra_args):
        def _do_download(chunk):
            t = 1  # sleep time 1,2,4,8,16...120
            for r in range(0, 10):
                try:
                    response = self.client.get_object(
                        Bucket=bucket, Key=key,
                        Range=chunk.range_param, **extra_args)
                    data = response['Body'].read()
                    assert len(data) == chunk.size
                    chunk.write(data)

                    self._add_finished_size(chunk.size)
                    LOGGER.debug("download %s %s" % (key, chunk.range_param))

                    if self._progress:
                        self._callback(self._total_size, self._finished_size)

                    return True
                except:
                    LOGGER.warn("retry %s part %s, %s of %s" % (
                            key, chunk.range_param, r+1, self._retry))
                    self._sleep_with(t)
                    t = t + 1
                    continue

            raise(RetriesExceededError("download retries exceeded"))

        return _do_download

    def _upload(self, src, bucket, key, extra_args):
        reader = FileChunkReader(src)
        self._total_size = reader.get_size()
        chunks = reader.get_chunks()
        self._parts_number = len(chunks)

        #  upload small file by using put_object.
        if self._parts_number == 1:
            chunk = chunks[0]
            LOGGER.info("%s is not need using MultipartUpload." % key)
            self.client.put_object(Bucket=bucket, Key=key,
                                   Body=chunk.read(), **extra_args)

            self._add_finished_size(chunk.size)
            if self._progress:
                self._callback(self._total_size, self._finished_size)
            LOGGER.info("upload %s finished" % key)
            return True

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
            LOGGER.info("upload %s finished" % key)

        except (KeyboardInterrupt, Exception) as error:
            executor.shutdown()
            LOGGER.error("abort upload %s %s %s" % (src, bucket, key))
            self.client.abort_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload_id)
            raise(error)
        return True

    def _upload_one_part(self, bucket, key, upload_id, extra_args):
        def _do_upload(chunk):
            t = 1  # sleep time 1,2,4,8,16...120
            for r in range(0, self._retry):
                try:
                    response = self.client.upload_part(
                        Bucket=bucket, Key=key,
                        UploadId=upload_id, PartNumber=chunk.part_number,
                        Body=chunk.read(), **extra_args)
                    etag = response['ETag']
                    self._add_finished_size(chunk.size)
                    LOGGER.debug("upload %s %s of %s" % (
                             key, chunk.part_number, self._parts_number))

                    if self._progress:
                        self._callback(self._total_size, self._finished_size)

                    return {'ETag': etag, 'PartNumber': chunk.part_number}
                except:
                    LOGGER.warn("retry %s part %s, %s of %s" % (
                             key, chunk.part_number, r+1, self._retry))
                    self._sleep_with(t)
                    t = t + 1
                    continue

            raise(RetriesExceededError("upload retries exceeded"))

        return _do_upload

    def _add_finished_size(self, size):
        if rlock.acquire():
            self._finished_size += size
            rlock.release()

    def _sleep_with(self, t):
        time.sleep(min(2**t, 120))


def upload_callback(total_size, finished_size):
    assert total_size > 0
    sys.stdout.write("\r%2.2f%%" % (finished_size*100/float(total_size)))
    sys.stdout.flush()


def s3copy():
    parser = argparse.ArgumentParser(
        description='upload and download file for aws s3')
    parser.add_argument(
        '-r', '--retry', type=int, help='retries for each chunk')
    parser.add_argument('-p', '--progress', action='store_true',
                        help='show transfer progress in percentage')
    parser.add_argument(
            '-v', '--verify', action='store_true',
            help='verify local and remote files with s3 object etag')
    parser.add_argument(
            '-t', '--threads', type=int, help='number of worker threads')
    parser.add_argument('src', nargs='?')
    parser.add_argument('dest')

    parser.add_argument('-e', '--etag', action='store_true',
                        help='get Etag of a object')

    args = parser.parse_args()

    src = args.src
    dest = args.dest
    etag = args.etag
    verify = args.verify
    progress = args.progress

    retry = args.retry if args.retry else 10
    threads = args.threads if args.threads else 5

    transfer = FileTransfer(retry, progress, threads, upload_callback)

    if etag:
        etag = transfer.etag_file(dest)
        print "successful. Etag:\n%s %s" % (etag, dest)
        sys.exit(0)

    if verify:
        etag = transfer.verify_file(src, dest)
        if etag[0] == etag[1] and etag[0] != None:
            print "successful. Etag:\n%s both" % etag[0]
            sys.exit(0)
        else:
            print "failed. Etag:\n%s %s\n%s %s" % (
                    etag[0], src, etag[1], dest)
            sys.exit(-1)

    if not src:
        parser.print_help()
        sys.exit(0)

    print "copy %s to %s" % (src, dest)
    if transfer.copy_file(src, dest):
        print "successful."

if "__main__" == __name__:
    try:
        s3copy()
    except Exception as err:
        print err
        sys.exit(-1)
