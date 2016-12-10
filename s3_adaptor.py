#!/usr/bin/env python

import conf
from boto.s3.connection import S3Connection
import boto.s3.key
# print ' ' * 4, ' %s' %()

class BucketError(BaseException):
    pass

class SIIIAdaptor(object):
    """
        S3 utilities for up/down load files, and other general utilities such as
        list files, check file size, SUM(file size) in a bucket.
    """
    def __init__(self):

        self._aws_connection = S3Connection(
            conf.AWS_ACCESS_KEY_ID,
            conf.AWS_SECRET_ACCESS_KEY,
            # is_secure=None
            )
        self.bucket_name = conf.AWS_STORAGE_BUCKET_NAME
        try:
            rs = self._aws_connection.get_all_buckets()

        except boto.exception.S3ResponseError:
            print 'connection failed, could be something wrong with AWS credentials'
        else:
            print 'connection to AWS account established'

            if self._bucket_exists():
                self.bucket = self._aws_connection.get_bucket(self.bucket_name)
                print 'connection to bucket %s established' % self.bucket_name
            else:
                raise ValueError('bucket %s does not exist' %self.bucket_name)


    def _bucket_exists(self):
        return self._aws_connection.lookup(self.bucket_name) != None

    def _bucket_not_found(self):

        print "Error: bucket '%s' not found" % (self.bucket_name)

    def list_all_keys(self):

        for file_key in self.bucket.list():
            print file_key.name

    def list_folder_content(self,foldername):

        print ' ' * 4, 'Listing contents within folder %s' %(foldername)
        count = 0
        obj = self.bucket.list(prefix = foldername)
        for key in obj:
            print ' ' * 8,  key.name
            count += 1
        print 'Number of items with in the folder is: %s' %(count)
        return key.name, count

    def get_bucket_size(self):

        size = 0
        for key in self.bucket.list():
            size += key.size

        print ' The size of your bucket is: %s MB' %(size*1.0/1024/1024)

    def download_all(self):

        for file_key in self.bucket.list():

            file_key.get_contents_to_filename(conf.PATH_DOWNLOAD+file_key.name)

    def download_lastest(self):

        pass

    def download_specific(
        self,
        uri, # short descriptor, similar to s3 key
        digest, # pre-calculated md5 that is retrieved from db
        path=conf.PATH_DOWNLOAD, # local file path to download to
        ):

        try:
            key = self.bucket.get_key(uri)
        except Exception:
            print Exception,'Warning: S3 bucket has no file matching the given URI: \n %s' %(uri)
        else:
            try:
                key.get_contents_to_filename(uri)
            except OSError:
                print 'Download path error! '
    def upload_(self,filename):
        pass


if __name__ == '__main__':
    try:
        s = SIIIAdaptor()
    except AttributeError:
        print 'Fail to access bucket!'
    except ValueError,e:
        print 'Your AWS S3 doesn\'t have this bucket !\nOriginal Details as below: %s' %e.args
    else:
        pass
    #    s._list_keys()
        # s.download_all()
    #    s.download_lastest()
        # s.list_folder_content('1-json')
        # s.get_bucket_size()

        # s.download_specific(
        #     '1-json/json2/testdf.txt',
        #     '123456786',
        #     )
