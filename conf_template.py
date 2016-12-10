#!/usr/bin/env python

"""
Settings for s3_adaptor module.

# -*- coding: utf-8 -*-

"""
from __future__ import absolute_import, unicode_literals
import os

# aws related settings
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
AWS_STORAGE_BUCKET_NAME = ''
AWS_AUTO_CREATE_BUCKET = True
AWS_QUERYSTRING_AUTH = False
AWS_S3_SECURE_URLS = False  # use simple http, not https
AWS_S3_FILE_OVERWRITE = True

# path settings
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
PATH_UPLOAD = os.path.join(PROJECT_ROOT, 'upload')
PATH_DOWNLOAD = os.path.join(PROJECT_ROOT, 'download/')

# project specific settings:
FOLDER_NAME = ''
