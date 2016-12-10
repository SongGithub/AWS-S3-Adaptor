#!/usr/bin/env python

import conf
from boto.s3.connection import S3Connection
import boto.s3.key
import os,ntpath
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
            # only here to validate connection. no actual need to get all buckets
            rs = self._aws_connection.get_all_buckets()
        except boto.exception.S3ResponseError:
            print 'connection failed, could be something wrong with AWS credentials'
        else:
            print 'connection to AWS account established'

            if self._bucket_exists():
                self.bucket = self._aws_connection.get_bucket(self.bucket_name)
                print 'connection to bucket %s has been established' % self.bucket_name
            else:
                raise ValueError('bucket %s does not exist' %self.bucket_name)

    def _bucket_exists(self):
        return self._aws_connection.lookup(self.bucket_name) != None

    def list_all_keys(self):

        for file_key in self.bucket.list():
            print file_key.name

    def list_folder_keys(self,foldername):

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
#            print self.path_builder(conf.PATH_DOWNLOAD, file_key.name)
            print '\n Key name is %s' % (file_key.name)
            if not len(self.path_builder(conf.PATH_DOWNLOAD, file_key.name)[1]) == 0:
                file_key.get_contents_to_filename(conf.PATH_DOWNLOAD+file_key.name)

    def download_lastest(self):
        pass

    def download_folder_files(self):
        pass

    def download_file(self,
        uri, # short descriptor, similar to s3 key
        digest, # pre-calculated md5 that is retrieved from db
        local_folder_path=conf.PATH_DOWNLOAD, # local folder path to download to
        ):

        try:
            key = self.bucket.get_key(uri)
        except Exception,e:
            print Exception,'Warning: S3 bucket has no file matching the given URI: \n %s.\n%s' %(uri, e.args)
            return e
        else:
            # check etag(MD5 digest) of the on-cloud file. Only proceed to download if its etag matches the MD5
            # digest record sourced from our database (which is supplied here as an argument)
            if not key.etag[1:-1] == digest:
                print 'data digest match failed, so the file is corrupted'
                return [-1, 'data digest match failed, so the file is corrupted']

            try:
                self.path_builder(local_folder_path, uri)
                key.get_contents_to_filename(local_folder_path + uri)
            except OSError,e:
                print 'Download path error!'
            else:
                pass

    def upload_(self,filename):
        pass

    def path_builder(self,base_path,uri):
        # build a path if it is not existing yet
        dirname = ntpath.dirname(os.path.join(base_path,uri))
        basename = ntpath.basename(os.path.join(base_path,uri))
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        return dirname, basename


if __name__ == '__main__':
    try:
        s = SIIIAdaptor()
    except AttributeError:
        print 'Fail to access bucket!'
    except ValueError,e:
        print 'Your AWS S3 doesn\'t have this bucket !\nOriginal Details as below: %s' %(e.args)
    else:

        # s.list_all_keys()
        s.download_all()
    #    s.download_lastest()
        # s.list_folder_keys('1-json')
        # s.get_bucket_size()

        # s.download_file(
        #     '1-json/tester2/testdf.txt',
        #     'f5d1278e8109edd94e1e4197e04873b9000',
        #     )
