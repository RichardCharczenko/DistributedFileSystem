from flask import Flask, request
import threading
import copy
import atexit
from NameNode import Name_Node

global nn
POOL_TIME = 30
reportThread = threading.Thread()
lock = threading.Lock()
nn = Name_Node()
tolorence = 1

def create_app():
    app = Flask(__name__)

    def interrupt():
        global reportThread
        reportThread.cancel()

    def block_report():
        global nn
        global reportThread
        global tolorence
        with lock:
            print('----CHECK----')
            print(nn.blocklist)
            print('Pre check: ', nn.data_seen)
            nn.check_data_nodes(tolorence)
            print('Post check: ', nn.data_seen)
        reportThread = threading.Timer(POOL_TIME, block_report, ())
        reportThread.start()

    def reportStart():
        global reportThread
        reportThread = threading.Timer(POOL_TIME, block_report, ())
        reportThread.start()

    reportStart()
    atexit.register(interrupt)
    return app

app = create_app()

@app.route('/heartbeat')
def heartbeat_handler():
    data = request.args.to_dict(flat=False)
    data_node = 'http://' + request.remote_addr + ':5000/'
    if data == {}:
        nn.receive_block_report([], data_node)
    for _, block_list in data.items():
        nn.receive_block_report(block_list, data_node)
    return 'recieved heartbeat', 200

@app.route('/create')
def create_handler():
    data = request.args.to_dict(flat=False)
    LoB = {}
    for filename, filesize in data.items():
        LoB = nn.receive_create(str(filename), int(filesize[0]))
        if LoB == -1:
            return 'error', 400
        if LoB == -2:
            return 'error', 300 
    print(LoB)
    return LoB

@app.route('/read')
def read_handle():
    data = request.args.to_dict(flat=False)
    LoB = {}
    for filename in data:
        LoB = nn.receive_read(filename)
        if LoB == -1:
            return 'create error', 400
    return LoB

@app.route('/list')
def block_list():
    data = request.args.to_dict(flat=False) # {}
    for filename in data:
        LoB = nn.receive_read(filename)
    if LoB == -1:
        return 'list error', 400 
    return LoB

if __name__ == '__main__':
    app.run(host='0.0.0.0')
