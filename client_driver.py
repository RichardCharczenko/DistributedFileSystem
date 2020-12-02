from client import Client
from os import getenv


def run_driver():
    name_node_url = getenv('NAME_NODE_URL')
    s3_bucket_name = getenv('S3_BUCKET_NAME')
    aws_access_key = getenv('AWS_ACCESS_KEY')
    aws_secret_key = getenv('AWS_SECRET_GET')
    s3_input_data_filename = getenv('DATA_FILENAME_S3')

    c = Client(name_node_url=name_node_url,
               s3_bucket_name=s3_bucket_name,
               aws_access_key=aws_access_key,
               aws_secret_key=aws_secret_key,
               data_filename_s3=s3_input_data_filename)
    c.get_data()
    c.create_write(c.data_filename_local, 'test.txt', c.s3_data_size)


if __name__ == '__main__':
    run_driver()
