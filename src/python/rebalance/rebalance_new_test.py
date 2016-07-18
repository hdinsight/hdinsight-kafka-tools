import unittest
import random
import json
import rebalance_new

'''
To run a single unit test:
python rebalance_new_test.py RebalanceTests.test_reassignment_plan_HA_for_topic
'''

# Tests
class RebalanceTests(unittest.TestCase):
    brokers_info = {
        'wn30-foobar': '1017',
        'wn25-foobar': '1016',
        'wn7-foobar': '1008',
        'wn12-foobar': '1026',
        'wn11-foobar': '1020',
        'wn19-foobar': '1001',
        'wn13-foobar': '1028',
        'wn8-foobar': '1009',
        'wn16-foobar': '1013',
        'wn29-foobar': '1030',
        'wn5-foobar': '1014',
        'wn21-foobar': '1006'
        }

    topicInfo_lines = ['Topic:dummyTopic PartitionCount:13 ReplicationFactor:3 Configs:segment.bytes=104857600,cleanup.policy=compact,compression.type=uncompressed', ' Topic: dummyTopic Partition: 0 Leader: 1026 Replicas: 1026,1028,1014 Isr: 1026,1028,1014', ' Topic: dummyTopic Partition: 1 Leader: 1020 Replicas: 1020,1014,1017 Isr: 1020,1014,1017', ' Topic: dummyTopic Partition: 2 Leader: 1028 Replicas: 1028,1017,1013 Isr: 1028,1017,1013', ' Topic: dummyTopic Partition: 3 Leader: 1009 Replicas: 1009,1013,1009 Isr: 1009,1013,1009', ' Topic: dummyTopic Partition: 4 Leader: 1030 Replicas: 1030,1001,1009 Isr: 1030,1001,1009', ' Topic: dummyTopic Partition: 5 Leader: 1008 Replicas: 1008,1020,1016 Isr: 1008,1020,1016', ' Topic: dummyTopic Partition: 6 Leader: 1013 Replicas: 1013,1014,1008 Isr: 1013,1014,1008', ' Topic: dummyTopic Partition: 7 Leader: 1001 Replicas: 1001,1028,1016 Isr: 1001,1028,1016', ' Topic: dummyTopic Partition: 8 Leader: 1014 Replicas: 1014,1017,1008 Isr: 1014,1017,1008', ' Topic: dummyTopic Partition: 9 Leader: 1006 Replicas: 1006,1001,1026 Isr: 1006,1001,1026', ' Topic: dummyTopic Partition: 10 Leader: 1017 Replicas: 1017,1028,1016 Isr: 1017,1028,1016', ' Topic: dummyTopic Partition: 11 Leader: 1016 Replicas: 1016,1026,1017 Isr: 1016,1026,1017', ' Topic: dummyTopic Partition: 12 Leader: 1006 Replicas: 1006,1020,1016 Isr: 1006,1020,1016', ' Topic: dummyTopic Partition: 13 Leader: 1030 Replicas: 1030,1001,1026 Isr: 1030,1001,1026']

    cluster_topo = '''
{
    "hostGroups": {
        "headnode": [
            {
                "vmId": 0,
                "fqdn": "hn0-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/headnode-0"
            }
        ],
        "workernode": [
            {
                "vmId": 0,
                "fqdn": "wn30-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 1,
                "fqdn": "wn25-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 10,
                "fqdn": "wn7-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 11,
                "fqdn": "wn12-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 12,
                "fqdn": "wn11-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 2,
                "fqdn": "wn19-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 3,
                "fqdn": "wn13-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 4,
                "fqdn": "wn8-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 5,
                "fqdn": "wn16-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 6,
                "fqdn": "wn29-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 7,
                "fqdn": "wn5-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 8,
                "fqdn": "wn21-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            }
        ],
        "zookeepernode": [
            {
                "vmId": 0,
                "fqdn": "zk0-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/zookeepernode-0"
            }
        ]
    }
}
        '''

    def test_parse_topo_info(self):
        host_info = rebalance_new.parse_topo_info(self.cluster_topo, self.brokers_info)
        broker_1020 = [element for element in host_info if int(element[rebalance_new.BROKER_ID]) == 1020][0]
       	self.assertEqual(broker_1020[rebalance_new.VM_ID], 12)
        self.assertEqual(broker_1020[rebalance_new.FQDN], 'wn11-foobar')
        self.assertEqual(broker_1020[rebalance_new.RACK], 'FD1UD1')
    
    def test_get_partition_info(self):
        partitions_info = rebalance_new.get_partition_info(self.topicInfo_lines)
        partition_4 = [element for element in partitions_info if int(element[rebalance_new.PARTITION]) == 4][0]
        self.assertEqual(partition_4[rebalance_new.PARTITION], 4)
        self.assertEqual(partition_4[rebalance_new.LEADER], 1030)
        self.assertEqual(set(partition_4[rebalance_new.REPLICAS]), set([1030, 1001, 1009]))
        self.assertEqual(set(partition_4[rebalance_new.ISR]), set([1030, 1001, 1009]))
    
    def test_generate_fd_list_ud_list(self):
        host_info = rebalance_new.parse_topo_info(self.cluster_topo, self.brokers_info)
        fd_list, ud_list = rebalance_new.generate_fd_list_ud_list(host_info)
        self.assertEqual(set(fd_list), set(['0','1','2']))
        self.assertEqual(set(ud_list), set(['0','1','2']))
    
    def test_generate_fd_ud_list(self):
        host_info = [{'vmId': 37, 'fqdn': 'wn25-foobar', 'updateDomain': '1', 'brokerId': '1016', 'faultDomain': '1', 'rack': 'FD1UD1'},{'vmId': 1, 'fqdn': 'wn25-foobar', 'updateDomain': '1', 'brokerId': '1016', 'faultDomain': '1', 'rack': 'FD1UD1'},{'vmId': 10, 'fqdn': 'wn7-foobar', 'updateDomain': '1', 'brokerId': '1008', 'faultDomain': '2', 'rack': 'FD2UD1'}]
        
        rgen = rebalance_new.ReassignmentGenerator(host_info, "dummyTopic", "partitionInfo")
        fd_ud_list = rgen._generate_fd_ud_list()
        self.assertEqual(fd_ud_list, ['FD1UD1', 'FD2UD1'])

    # 3 FDs, 5 UDs
    def test_generate_alternated_fd_ud_list_test_case1(self):
        fd_ud_list = ['FD0UD0', 'FD0UD1', 'FD0UD2', 'FD0UD3', 'FD0UD4', 'FD1UD0', 'FD1UD1', 'FD1UD2', 'FD1UD3', 'FD1UD4', 'FD2UD0', 'FD2UD1', 'FD2UD2','FD2UD3', 'FD2UD4']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1', '2', '3', '4']
        rgen = rebalance_new.ReassignmentGenerator("hostInfo", "dummyTopic", "partitionInfo")
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD3', 'FD1UD4', 'FD2UD0', 'FD0UD1', 'FD1UD2', 'FD2UD3', 'FD0UD4', 'FD1UD0', 'FD2UD1', 'FD0UD2', 'FD1UD3', 'FD2UD4']
        self.assertEqual(alternated_list, expected_list)

    # Siphon 3 FDs, 5 UDs (k09v3)
    def test_generate_alternated_fd_ud_list_test_case2(self):
        fd_ud_list = ['FD0UD0', 'FD0UD1', 'FD0UD2', 'FD0UD3','FD0UD4', 'FD1UD0', 'FD1UD1', 'FD1UD2', 'FD1UD3', 'FD1UD4', 'FD2UD0', 'FD2UD1', 'FD2UD2', 'FD2UD3', 'FD2UD4']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1', '2', '3', '4']
        rgen = rebalance_new.ReassignmentGenerator("hostInfo", "dummyTopic", "partitionInfo")
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD3', 'FD1UD4', 'FD2UD0', 'FD0UD1', 'FD1UD2', 'FD2UD3', 'FD0UD4', 'FD1UD0', 'FD2UD1', 'FD0UD2', 'FD1UD3', 'FD2UD4']
        self.assertEqual(alternated_list, expected_list)
    
    #3 FDs, 2 UDs
    def test_generate_alternated_fd_ud_list_test_case3(self):
        fd_ud_list = ['FD0UD0', 'FD1UD1', 'FD2UD0', 'FD0UD1']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1']
        rgen = rebalance_new.ReassignmentGenerator("hostInfo", "dummyTopic", "partitionInfo")
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD0', 'FD0UD1']
        self.assertEqual(alternated_list, expected_list)

    #3 FDs, 3UDs
    def test_generate_alternated_fd_ud_list_test_case4(self):
        fd_ud_list = ['FD0UD0', 'FD0UD1', 'FD0UD2', 'FD1UD0', 'FD1UD1', 'FD1UD2', 'FD2UD0', 'FD2UD1', 'FD2UD2']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1', '2']
        rgen = rebalance_new.ReassignmentGenerator("hostInfo", "dummyTopic", "partitionInfo")
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD1', 'FD1UD2', 'FD2UD0', 'FD0UD2', 'FD1UD0', 'FD2UD1']
        self.assertEqual(alternated_list, expected_list)

    #3 FDs, 3UDs with Gaps
    def test_generate_alternated_fd_ud_list_test_case5(self):
        fd_ud_list = ['FD0UD1', 'FD0UD2', 'FD1UD0', 'FD1UD1', 'FD2UD0', 'FD2UD2']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1', '2']
        rgen = rebalance_new.ReassignmentGenerator("hostInfo", "dummyTopic", "partitionInfo")
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD1UD1', 'FD2UD2', 'FD0UD1', 'FD2UD0', 'FD0UD2', 'FD1UD0']
        self.assertEqual(alternated_list, expected_list)
    
    #3 FDs, 6UDs (n, nm)
    def test_generate_alternated_fd_ud_list_test_case6(self):
        fd_ud_list = ['FD0UD0', 'FD0UD1', 'FD0UD2', 'FD0UD3', 'FD0UD4', 'FD0UD5', 'FD1UD0', 'FD1UD1', 'FD1UD2', 'FD1UD3', 'FD1UD4', 'FD1UD5', 'FD2UD0', 'FD2UD1', 'FD2UD2','FD2UD3', 'FD2UD4', 'FD2UD5']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1', '2', '3', '4', '5']
        rgen = rebalance_new.ReassignmentGenerator("hostInfo", "dummyTopic", "partitionInfo")
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD1', 'FD1UD2', 'FD2UD3', 'FD0UD2', 'FD1UD3', 'FD2UD4', 'FD0UD3', 'FD1UD4', 'FD2UD5', 'FD0UD4', 'FD1UD5', 'FD2UD0', 'FD0UD5', 'FD1UD0', 'FD2UD1']
        self.assertEqual(alternated_list, expected_list)

    def test_reassignment_plan_HA_for_topic(self):
        host_info = rebalance_new.parse_topo_info(self.cluster_topo, self.brokers_info)
        partitions_info = rebalance_new.get_partition_info(self.topicInfo_lines)
        rgen = rebalance_new.ReassignmentGenerator(host_info, "dummyTopic", partitions_info)
        reassignment_plan = rgen._generate_reassignment_plan_for_topic(3)
        topic_balanced = self.check_reassignment_plan(reassignment_plan, host_info)
        self.assertTrue(topic_balanced)
    
    def check_reassignment_plan(self, reassignment_plan, host_info):
        partitions_plan = reassignment_plan["partitions"]
        for p in partitions_plan:
            fd_ud_list = []
            for replica in p[rebalance_new.REPLICAS]:
                fd_ud_list.append(self.get_broker_info(int(replica), host_info)[rebalance_new.RACK])
            if len(fd_ud_list) > len(set(fd_ud_list)):
                return False
        return True
    
    def get_broker_info(self, b_id, host_info):
        return [element for element in host_info if int(element[rebalance_new.BROKER_ID]) == b_id][0]
            
def main():
    unittest.main()

if __name__ == '__main__':
    main()