import boto3
import os
from math import ceil
import requests


class ClientNode:
    def __init__(self, s3_bucket, name_node_ip, aws_access_key, aws_secret_key, bucket_name, object_name):
        self.s3_bucket = s3_bucket
        self.name_node_ip = name_node_ip
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.bucket_name = bucket_name
        self.object_name = object_name

    def get_data(self, s3_bucket):
        # Connect to s3:
        s3 = boto3.client('s3', region_name='us-west2', aws_access_key=self.aws_access_key,
                                   aws_secret_key=self.aws_secret_key)

        # Download file from s3 bucket:
        s3.download_file(self.bucket_name, self.object_name, 'data.txt')

    def break_into_blocks(self, fn, block_size):
        blocks = {}
        file_size = os.stat(fn).st_size
        number_of_blocks = ceil(file_size/131072)

        with open(fn, 'rb') as f:
            for block in range(number_of_blocks):
                b = open('block' + str(block), 'wb')
                b.write(fn.read(131072))
                blocks[block] = b
                b.close()
        return blocks

    def create_write(self, fn, size):
        # Write new file to SUFS
        pass

    def read(self, fn):
        # Write f to local disk
        pass

    def list(self, fn):
        # Given f, display LoB on terminal

        schema = 'http://'
        IP = self.name_node_ip
        extension = '/list'
        URL = schema + IP + extension

        parameters = {'filename': fn}
        response = requests.get(url=URL, params=parameters)
        data = response.json()

        # Print data

        return data

