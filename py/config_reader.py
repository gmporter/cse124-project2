import re
import sys

'''A simple class to read SurfStore configuration files.

You shouldn't need to edit this file. If there's a bug, please contact a TA!'''
class SurfStoreConfigReader(object):
    num_metadata_match_str="M(:|=)\s*(?P<num_metadata>\d+)"
    num_leader_match_str="L(:|=)\s*(?P<num_leader>\d+)"
    metadata_inst_match_str="metadata(?P<metadata_id>\d+)(:|=)\s*(?P<metadata_port>\d+)"
    block_inst_match_str="block(:|=)\s*(?P<block_port>\d+)"
    config_matcher=re.compile("((%s)|(%s)|(%s)|(%s))\s*" % (
        num_metadata_match_str,
        num_leader_match_str,
        metadata_inst_match_str,
        block_inst_match_str
    ))

    def __init__(self, config_file):
        self.config_file = config_file
        self.metadata_ports = {}

        with open(config_file, "r") as f:
            for line in f:
                result = self.config_matcher.match(line).groupdict()
                if result["num_metadata"] is not None:
                    self.num_metadata_servers = int(result["num_metadata"])
                elif result["num_leader"] is not None:
                    self.num_leaders = int(result["num_leader"])
                elif result["metadata_id"] is not None:
                    self.metadata_ports[int(result["metadata_id"])] = int(result["metadata_port"])
                elif result["block_port"] is not None:
                    self.block_port = int(result["block_port"])
                else:
                    print >> sys.stderr, "%s: Invalid line:\n%s" % (self.__class__.__name__, line)

        if not hasattr(self, "num_metadata_servers") \
        or not hasattr(self, "block_port") \
        or not self.metadata_ports:
            raise Exception("Config file is missing one or more required lines!")

        for i in range(1, self.num_metadata_servers + 1):
            if not self.metadata_ports[i]:
                raise Exception("Must set port for metadata%d" % i)

    def get_num_metadata_servers(self):
        return self.num_metadata_servers

    def get_metadata_port(self, server_id):
        return self.metadata_ports[server_id]

    def get_block_port(self):
        return self.block_port