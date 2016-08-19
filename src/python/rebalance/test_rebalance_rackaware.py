import unittest
import random
import json
import rebalance_rackaware

'''
To run a single unit test:
python rebalance_rackaware_test.py RebalanceTests.test_reassignment_plan_HA_for_topic
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
    
    compute_storage_cost = False
    dummy_topic = "dummyTopic"

    def test_parse_topo_info(self):
        host_info = rebalance_rackaware.parse_topo_info(self.cluster_topo, self.brokers_info)
        broker_1020 = [element for element in host_info if int(element[rebalance_rackaware.BROKER_ID]) == 1020][0]
       	self.assertEqual(broker_1020[rebalance_rackaware.VM_ID], 12)
        self.assertEqual(broker_1020[rebalance_rackaware.FQDN], 'wn11-foobar')
        self.assertEqual(broker_1020[rebalance_rackaware.RACK], 'FD1UD1')
    
    def test_get_partition_info(self):
        partitions_info = rebalance_rackaware.get_partition_info(self.topicInfo_lines, [], self.dummy_topic)
        partition_4 = [element for element in partitions_info if int(element[rebalance_rackaware.PARTITION]) == 4][0]
        self.assertEqual(partition_4[rebalance_rackaware.PARTITION], 4)
        self.assertEqual(partition_4[rebalance_rackaware.LEADER], 1030)
        self.assertEqual(set(partition_4[rebalance_rackaware.REPLICAS]), set([1030, 1001, 1009]))
        self.assertEqual(set(partition_4[rebalance_rackaware.ISR]), set([1030, 1001, 1009]))
    
    def test_generate_fd_list_ud_list(self):
        host_info = rebalance_rackaware.parse_topo_info(self.cluster_topo, self.brokers_info)
        fd_list, ud_list = rebalance_rackaware.generate_fd_list_ud_list(host_info)
        self.assertEqual(set(fd_list), set(['0','1','2']))
        self.assertEqual(set(ud_list), set(['0','1','2']))
    
    def test_generate_fd_ud_list(self):
        host_info = [{'vmId': 37, 'fqdn': 'wn25-foobar', 'updateDomain': '1', 'brokerId': '1016', 'faultDomain': '1', 'rack': 'FD1UD1'},{'vmId': 1, 'fqdn': 'wn25-foobar', 'updateDomain': '1', 'brokerId': '1016', 'faultDomain': '1', 'rack': 'FD1UD1'},{'vmId': 10, 'fqdn': 'wn7-foobar', 'updateDomain': '1', 'brokerId': '1008', 'faultDomain': '2', 'rack': 'FD2UD1'}]   
        rgen = rebalance_rackaware.ReassignmentGenerator(host_info, self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        fd_ud_list = rgen._generate_fd_ud_list()
        self.assertEqual(fd_ud_list, ['FD1UD1', 'FD2UD1'])

    # 3 FDs, 5 UDs
    def test_generate_alternated_fd_ud_list_test_case_3FDs_5UDs(self):
        (FDs, UDs) = 3,5
        fd_ud_list, fd_list, ud_list = self.generate_fd_ud_list(FDs,UDs)
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD3', 'FD1UD4', 'FD2UD0', 'FD0UD1', 'FD1UD2', 'FD2UD3', 'FD0UD4', 'FD1UD0', 'FD2UD1', 'FD0UD2', 'FD1UD3', 'FD2UD4']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)

    #3 FDs, 2 UDs
    def test_generate_alternated_fd_ud_list_test_case_3FDs_2UDs(self):
        fd_ud_list = ['FD0UD0', 'FD0UD1', 'FD1UD0', 'FD1UD1', 'FD2UD0', 'FD2UD1']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1']
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD0', 'FD0UD1', 'FD1UD0', 'FD2UD1']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)
    
    #3 FDs, 2 UDs with gaps
    def test_generate_alternated_fd_ud_list_test_case_3FDs_2UDs_with_gaps(self):
        fd_ud_list = ['FD0UD0', 'FD1UD1', 'FD2UD0', 'FD0UD1']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1']
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD0', 'FD0UD1']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)

    #3 FDs, 3UDs
    def test_generate_alternated_fd_ud_list_test_case_3FDs_3UDs(self):
        (FDs, UDs) = 3,3
        fd_ud_list, fd_list, ud_list = self.generate_fd_ud_list(FDs,UDs)
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD1', 'FD1UD2', 'FD2UD0', 'FD0UD2', 'FD1UD0', 'FD2UD1']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)

    #3 FDs, 3UDs with Gaps
    def test_generate_alternated_fd_ud_list_test_case_3FDs_3UDs_with_gaps(self):
        fd_ud_list = ['FD0UD1', 'FD0UD2', 'FD1UD0', 'FD1UD1', 'FD2UD0', 'FD2UD2']
        fd_list = ['0', '1', '2']
        ud_list = ['0', '1', '2']
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD1UD1', 'FD2UD2', 'FD0UD1', 'FD2UD0', 'FD0UD2', 'FD1UD0']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)
    
    #3 FDs, 6UDs (n, nm)
    def test_generate_alternated_fd_ud_list_test_case_3FDs_6UDs(self):
        (FDs, UDs) = 3,6
        fd_ud_list, fd_list, ud_list = self.generate_fd_ud_list(FDs,UDs)
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD0UD1', 'FD1UD2', 'FD2UD3', 'FD0UD2', 'FD1UD3', 'FD2UD4', 'FD0UD3', 'FD1UD4', 'FD2UD5', 'FD0UD4', 'FD1UD5', 'FD2UD0', 'FD0UD5', 'FD1UD0', 'FD2UD1']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)

    #4 FDs, 6UDs (n, nm)
    def test_generate_alternated_fd_ud_list_test_case_4FDs_6UDs(self):
        (FDs, UDs) = 4,6
        fd_ud_list, fd_list, ud_list = self.generate_fd_ud_list(FDs,UDs)
        rgen = rebalance_rackaware.ReassignmentGenerator("hostInfo", self.dummy_topic, "partitionInfo", self.compute_storage_cost)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        expected_list = ['FD0UD0', 'FD1UD1', 'FD2UD2', 'FD3UD3', 'FD0UD1', 'FD1UD2', 'FD2UD3', 'FD3UD4', 'FD0UD2', 'FD1UD3', 'FD2UD4', 'FD3UD5', 'FD0UD3', 'FD1UD4', 'FD2UD5', 'FD3UD0', 'FD0UD4', 'FD1UD5', 'FD2UD0', 'FD3UD1', 'FD0UD5', 'FD1UD0', 'FD2UD1', 'FD3UD2']
        self.assertEqual(len(alternated_list), len(fd_ud_list))
        self.assertEqual(alternated_list, expected_list)

    def test_reassignment_plan_HA_for_topic(self):
        host_info = rebalance_rackaware.parse_topo_info(self.cluster_topo, self.brokers_info)
        brokers_replica_count = []
        for host in host_info:
            b = {
                rebalance_rackaware.BROKER_ID : host[rebalance_rackaware.BROKER_ID],
                rebalance_rackaware.LEADERS : 0,
                rebalance_rackaware.FOLLOWERS: 0,
            }
            brokers_replica_count.append(b)
        partitions_info = rebalance_rackaware.get_partition_info(self.topicInfo_lines, [], self.dummy_topic)
        rgen = rebalance_rackaware.ReassignmentGenerator(host_info, self.dummy_topic, partitions_info, self.compute_storage_cost)
        fd_ud_list, fd_list, ud_list = self.generate_fd_ud_list(3,3)
        alternated_list = rgen._generate_alternated_fd_ud_list(fd_ud_list, fd_list, ud_list)
        reassignment_plan, balanced_partitions = rgen._generate_reassignment_plan_for_topic(3,0,alternated_list,3,3,brokers_replica_count, None)
        topic_balanced = rgen._verify_reassignment_plan(reassignment_plan,self.dummy_topic,3,3,3)
        self.assertTrue(topic_balanced)

    def test_check_if_partition_balanced(self):
        host_info = [{'vmId': 1, 'fqdn': 'wn01-foobar', 'updateDomain': '0', 'brokerId': '1016', 'faultDomain': '1', 'rack': 'FD1UD0'},
                    {'vmId': 2, 'fqdn': 'wn02-foobar', 'updateDomain': '2', 'brokerId': '1017', 'faultDomain': '0', 'rack': 'FD0UD2'},
                    {'vmId': 3, 'fqdn': 'wn77-foobar', 'updateDomain': '1', 'brokerId': '1008', 'faultDomain': '2', 'rack': 'FD2UD1'},
                    {'vmId': 4, 'fqdn': 'wn47-foobar', 'updateDomain': '2', 'brokerId': '1098', 'faultDomain': '2', 'rack': 'FD2UD2'},
                    {'vmId': 5, 'fqdn': 'wn56-foobar', 'updateDomain': '1', 'brokerId': '2', 'faultDomain': '2', 'rack': 'FD2UD1'}]
        rgen = rebalance_rackaware.ReassignmentGenerator(host_info, self.dummy_topic, "partitions", self.compute_storage_cost)

        partition_1 = {"partition": 1,"leader": 1016,"replicas": [1016, 1017, 1008],"isr": [1016, 1017, 1008]}
        partition_2 = {"partition": 1,"leader": 2,"replicas": [1016, 2, 1098],"isr": [1016]}
        partition_3 = {"partition": 1,"leader": 2,"replicas": [1016, 2],"isr": [1016]}
        partition_4 = {"partition": 1,"leader": 2,"replicas": [1016, 1017, 1008, 1098],"isr": [1016]}
        partition_5 = {"partition": 1,"leader": 2,"replicas": [1008, 1098],"isr": [1008]}
        partition_6 = {"partition": 1,"leader": 2,"replicas": [1016, 1017],"isr": [1008]}
        
        #Balanced Checks
        self.assertTrue(rgen._check_if_partition_balanced(partition_1,3,3,3,[],[]))
        self.assertTrue(rgen._check_if_partition_balanced(partition_3,2,3,3,[],[]))
        self.assertTrue(rgen._check_if_partition_balanced(partition_4,4,3,3,[],[]))

        #Imbalanced Checks
        self.assertFalse(rgen._check_if_partition_balanced(partition_2,3,3,3,[],[]))
        self.assertFalse(rgen._check_if_partition_balanced(partition_5,2,3,3,[],[]))
        self.assertFalse(rgen._check_if_partition_balanced(partition_6,3,3,3,[],[])) 
    
    def get_broker_info(self, b_id, host_info):
        return [element for element in host_info if int(element[rebalance_rackaware.BROKER_ID]) == b_id][0]

    def generate_fd_ud_list(self, m, n):
        fd_ud_list, fd_list, ud_list = [], [], set()
        for i in range(0, m):
            fd_list.append(str(i))
            for j in range(0, n):
                ud_list.add(str(j))
                fd_ud_list.append('FD' + str(i) + 'UD' + str(j))

        self.assertEqual(len(fd_ud_list), m*n)
        self.assertEqual(len(fd_list), m)
        self.assertEqual(len(ud_list), n)        
        return fd_ud_list, fd_list, sorted(ud_list)
            
def main():
    unittest.main()

if __name__ == '__main__':
    main()