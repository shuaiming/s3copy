# s3copy
	~$ python s3copy.py -h
	usage: s3copy.py [-h] [-r RETRY] [-p] [-t THREADS] src dest
	
	upload and download file for aws s3
	
	positional arguments:
	  src
	  dest
	
	optional arguments:
	  -h, --help            show this help message and exit
	  -r RETRY, --retry RETRY
	                        retries for each chunk
	  -p, --progress        show transfer progress in percentage
	  -t THREADS, --threads THREADS
	                        number of worker threads

# todo
*	verifying the file checksum by using __multipartupload__ ETag.
*	working with io streams to reduce memory usage.
*	bucket to bucket transfer?
