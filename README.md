# s3copy
	~$ python s3copy.py -h
    usage: s3copy.py [-h] [-r RETRY] [-p] [-v] [-t THREADS] [-e] [src] dest
    
    upload and download file for aws s3
    
    positional arguments:
    src
    dest
    
    optional arguments:
    -h, --help            show this help message and exit
    -r RETRY, --retry RETRY
                          retries for each chunk
    -p, --progress        show transfer progress in percentage
    -v, --verify          verify local and remote files with s3 object etag
    -t THREADS, --threads THREADS
                          number of worker threads
    -e, --etag            get Etag of a object
# todo
*	working with io streams to reduce memory usage.
