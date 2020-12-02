from flask import Flask, request, url_for, jsonify
import requests
import threading
import time
import os
import atexit

POOL_TIME = 30 # Seconds
# variables that are accessible from anywhere
DATABLOCKS = []
# lock to control access to variable
dataLock = threading.Lock()
# thread handler
heartThread = threading.Thread()
# url for namenode
NAMENODE = 'http://35.162.89.185:5000'

def create_app():
    app = Flask(__name__)

    def interrupt():
        global heartThread
        heartThread.cancel()

    def heartbeat():
        global DATABLOCKS
        global heartThread
        global NAMENODE
        with dataLock:
            block_report = {'blocks': DATABLOCKS}
            response = requests.get(url=NAMENODE+'/heartbeat', params = block_report)
        # Set the next thread to happen
        heartThread = threading.Timer(POOL_TIME, heartbeat, ())
        heartThread.start()

    def heartbeatStart():
        # Do initialisation stuff here
        global heartThread
        # Create your thread
        heartThread = threading.Timer(POOL_TIME, heartbeat, ())
        heartThread.start()

    # Initiate
    heartbeatStart()
    # When you kill Flask (SIGTERM), clear the trigger for the next thread
    atexit.register(interrupt)
    return app

app = create_app()


@app.route('/write', methods=['GET', 'POST', 'PUT'])
def write_file():
    global DATABLOCKS
    data = request.files.to_dict(flat=False)
    for f_name in data:
        if f_name in DATABLOCKS:
            return 'block already in node', 400
        if f_name == '':
            return 'invalid file name', 400
        file = request.files[f_name]
        DATABLOCKS += [f_name]
        file.save(f_name)
    return 'completed', 200


@app.route('/forward')
def forward_to():
    data = request.args.to_dict(flat=False) # expect {url_to_send_to:block_id}
    for url in data:
        filename = "".join(data[url])
        with open(filename, 'rb') as block: # open file
            response = requests.post(url+'write', files={filename: block})
    return 'complete', 200


@app.route('/read')
def read():
    data = request.args.to_dict(flat=False)
    file_name = "".join(data['filename'])
    try:
        with open(file_name, 'rb') as block:
            block_data = block.read()
    except FileNotFoundError:
        return 'Record not found', 400
    return block_data


if __name__ == '__main__':
    # break down
    app.run(host='0.0.0.0')