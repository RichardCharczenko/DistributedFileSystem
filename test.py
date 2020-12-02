from NameNode import Name_Node


def main():
    data_nodes = ["url1", "url2", "url3", "url4", "url5", "url6", "url7", "url8", "url9", "url10"]
    name_node = Name_Node(data_nodes)
    block_rep = ["test_0"]

    print("Create: ")
    print(name_node.receive_create("test", 4000000))
    print("Receive block report: ")
    print(name_node.receive_block_report(block_rep, "url2"))
    print("Data seen: ")
    print(name_node.data_seen)
    print("Blocklist: ")
    print(name_node.blocklist)
    print("call from remove data block: ")
    print(name_node.remove_data_node('url2'))
    print(name_node.blocklist)
    print(name_node.data_seen)
    print(name_node.data_nodes)

if __name__ == "__main__":
    main()
