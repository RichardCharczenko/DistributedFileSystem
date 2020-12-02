import math
import hashlib
import requests

class Name_Node:

    def __init__(self, data_nodes=[], replication=3, size=134217728, freq=30):
        self.blocklist = {}  # dict key = filename, val  = dict of <k,v> pairs
        # key = block_id, value = list of data nodes
        self.data_seen = {}  # dict key = node, val = int val, 0 means it has been seen recently,
        # 1 means it hasn't been seen for 30 sec, 2 means it hasn't been seen for 60 sec
        for node in data_nodes:
            self.data_seen[node] = 0

        self.data_nodes = data_nodes  # arr where index is bucket num, and arr[index] = urls
        self.replication = replication
        self.block_size = size
        self.frequency = freq

    def receive_create(self, fn, size):
        print(type(fn), fn)
        print(type(size), size) 

        if len(self.data_nodes) < self.replication:
            print(self.data_nodes)
            return -1
        if fn in self.blocklist:
            print(self.blocklist)
            return -2 

        self.blocklist[fn] = {}
        #num_of_blocks = int((math.ceil(size / self.block_size))
        num_of_blocks = -(-size//self.block_size)
        print(num_of_blocks)
        list_of_blocks = {}
        for n in range(num_of_blocks):
            block_id = fn + '_' + str(n)
            bucket_num = hash(block_id) % len(self.data_nodes)
            list_of_blocks[block_id] = [self.data_nodes[bucket_num]]

            if block_id not in self.blocklist[fn]:
                self.blocklist[fn][block_id] = []

            for copy in range(1, self.replication):
                if bucket_num + copy <= len(self.data_nodes) - 1:
                    list_of_blocks[block_id].append(self.data_nodes[bucket_num + copy])

                else:
                    list_of_blocks[block_id].append(self.data_nodes[bucket_num - copy])

        return list_of_blocks

    def receive_read(self, filename):
        if filename in self.blocklist:
            return self.blocklist[filename]
        print(filename)
        return -1

    def receive_block_report(self, block_report, data_node_id):
        if data_node_id not in self.data_nodes:
            self.data_nodes.append(data_node_id)
        self.data_seen[data_node_id] = 0

        for block in block_report:
            fn = '_'.join(block.split('_')[:-1])
            if fn in self.blocklist:
                if data_node_id not in self.blocklist[fn][block]: 
                    self.blocklist[fn][block].append(data_node_id)

    def check_replication(self):
        for data_list in self.blocklist.values():
            for block_id in data_list:
                node_list = data_list[block_id]
                if len(node_list) < self.replication:
                    num_copies = self.replication - len(node_list)

                    messenger_node = node_list[0]
                    new_nodes = []

                    while(num_copies > 0):
                        for node in self.data_nodes:
                            if node not in node_list:
                                new_nodes += [node]
                                num_copies -= 1
                    for node in new_nodes:
                        PARAMS = {node: block_id}
                        response = requests.get(url=messenger_node+'forward', params=PARAMS)

    def remove_data_node(self, data_node_id):
        for file, data_list in self.blocklist.items():
            for node_list in data_list.values():
                if data_node_id in node_list:
                    node_list.remove(data_node_id)

        if data_node_id in self.data_nodes:
            self.data_nodes.remove(data_node_id)

        del self.data_seen[data_node_id]
        return

    def check_data_nodes(self, tolerance):
        for node in list(self.data_seen):
            if self.data_seen[node] >= tolerance:
                self.remove_data_node(node)
                self.check_replication()
        for node in self.data_seen:
            self.data_seen[node] += 1
        return


