import boto3
import os
from math import ceil
import requests


class Client:
    def __init__(self, name_node_url, aws_access_key, aws_secret_key, s3_bucket_name, data_filename_s3,
                 data_filename_local='data.txt', s3_region_name='us-west-2'):
        assert type(name_node_url) is str
        assert type(aws_access_key) is str
        assert type(aws_secret_key) is str
        assert type(s3_bucket_name) is str
        assert type(data_filename_s3) is str
        assert type(data_filename_local) is str
        assert type(s3_region_name) is str
        self.name_node_url = name_node_url
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.s3_bucket_name = s3_bucket_name
        self.data_filename_s3 = data_filename_s3
        self.data_filename_local = data_filename_local
        self.s3_region_name = s3_region_name
        self.s3_data_size = None
        self.s3_data_blocks = None

    def get_data(self):
        s3 = boto3.client('s3', region_name=self.s3_region_name, aws_access_key_id=self.aws_access_key,
                          aws_secret_access_key=self.aws_secret_key)

        # Save to data_filename_local:
        s3.download_file(self.s3_bucket_name, self.data_filename_s3, self.data_filename_local)
        self.s3_data_size = os.stat(self.data_filename_local).st_size

    @staticmethod
    def break_into_blocks(filename, LOB, block_size_bytes=134217728):
        blocks = {}
        file_size_bytes = os.stat(filename).st_size
        number_of_blocks = len(LOB)
        # -(-file_size_bytes // block_size_bytes)
        with open(filename, 'rb') as f:
            for block_filename in LOB:
                # block_filename = "{0}_{1}".format(filename, block)
                with open(block_filename, 'wb') as b:
                    b.write(f.read(block_size_bytes))
                    blocks[block_filename] = block_filename
        return blocks

    @staticmethod
    def write_block_to_data_nodes(block_filename, data_nodes):
        responses = []
        with open(block_filename, 'rb') as block:
            block_data = block.read()
        payload = {block_filename: block_data}
        print(payload)
        for url in data_nodes:
            node_url = Client.fix_extension(url, '/write')
            print(type(node_url), node_url)
            print(payload)
            r = requests.post(url=node_url, files=payload)
            responses += [r]
        all_uploads_failed = True
        for response in responses:
            if response.status_code == 200:
                all_uploads_failed = False
        if all_uploads_failed:
            raise Exception("all uploads for {0} failed".format(block_filename))
        return responses

    @staticmethod
    def fix_extension(url, extension):
        if url[-1] == '/':
            extension = extension[1:]
        return url + extension

    def create_write(self, local_filename, sufs_filename, size_in_bytes, block_size=2 ** 27):
        params = {sufs_filename: size_in_bytes}
        print(params)

        url = self.name_node_url + '/create'
        #print(self.name_node_url)

        response = requests.get(url, params)
        #print(response)

        if response.status_code != 200:
            #print(url)
            raise Exception("requests.get failed: status code {0}".format(response.status_code))
        LoB = response.json()
        print(LoB)
        blocks = self.break_into_blocks(local_filename, LoB, block_size)
        responses = []
        print(blocks)
        for _, block_filename in blocks.items():
            responses.append(self.write_block_to_data_nodes(
                block_filename, LoB[block_filename]))
        return responses

    @staticmethod
    def sort_keys(keys):
        sequence = {}
        for key in keys:
            sequence_number = int(key.split('_')[-1])
            sequence[sequence_number] = key
        sorted_keys = []
        for key in sorted(sequence.keys()):
            sorted_keys.append(sequence[key])
        return sorted_keys

    @staticmethod
    def merge_blocks_into_file(blocks, filename):
        key_order = Client.sort_keys(blocks.keys())
        data_list = []
        for key in key_order:
            data_list.append(blocks[key])
        with open(filename, 'wb') as f:
            for block_data in data_list:
                for line in block_data:
                    f.write(line)
        print(key_order)

    def read(self, sufs_filename, local_filename):
        # Read data from SUFS and save to local disk
        params = {sufs_filename: sufs_filename}
        url = self.fix_extension(self.name_node_url, '/read')
        response = requests.get(url, params)
        if response.status_code != 200:
            raise Exception('Name node response to read: {0}'.format(response.status_code))
        resp = response.json()
        blocks = {}
        print(resp)
        for block in resp:
            successfully_retrieved_block = False
            for url in resp[block]:
                url = self.fix_extension(url, '/read')
                print(url)
                params = {'filename': block}
                response = requests.get(url, params)
                if response.status_code == 200:
                    successfully_retrieved_block = True
                    blocks[block] = response
                    break
            if not successfully_retrieved_block:
                raise Exception('block {0} failed to download from all nodes'.format(block))
        self.merge_blocks_into_file(blocks, local_filename)

    def list(self, filename):
        url = self.fix_extension(self.name_node_url, '/list')
        parameters = {filename: filename}
        response = requests.get(url=url, params=parameters)
        if response.status_code != 200:
            raise Exception('name node returned status code {0}'.format(response.status_code))
        data = response.json()
        print(data)
        return data