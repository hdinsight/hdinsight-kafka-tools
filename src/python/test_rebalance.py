import unittest
import rebalance
import random
import json

'''
To run a single unit test:
python test_rebalance.py Test.test_no_conflict
'''
class Test(unittest.TestCase):
    def test_parse_partitions_info(self):
        a = '''Topic:test.2    PartitionCount:2        ReplicationFactor:3     Configs:
        Topic: test.2   Partition: 0    Leader: 2       Replicas: 2,0,1 Isr: 2,0,1
        Topic: test.2   Partition: 1    Leader: 1       Replicas: 1,2,3 Isr: 1,2,3
        '''
        r = rebalance.parse_partitions_info(a)
        self.assertEqual(r, [[2,0,1],[1,2,3]])

    def test_parse_topo_info(self):
        a = '''
{
    "hostGroups": {
        "gateway": [
            {
                "vmId": 0,
                "fqdn": "gw0-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/gateway-0"
            },
            {
                "vmId": 1,
                "fqdn": "gw1-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/gateway-0"
            }
        ],
        "headnode": [
            {
                "vmId": 0,
                "fqdn": "hn0-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/headnode-0"
            },
            {
                "vmId": 1,
                "fqdn": "hn1-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/headnode-0"
            }
        ],
        "workernode": [
            {
                "vmId": 0,
                "fqdn": "wn0-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 1,
                "fqdn": "wn1-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 10,
                "fqdn": "wn10-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 11,
                "fqdn": "wn11-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 12,
                "fqdn": "wn12-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 3,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 2,
                "fqdn": "wn2-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 3,
                "fqdn": "wn3-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 4,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 4,
                "fqdn": "wn4-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 5,
                "fqdn": "wn5-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 6,
                "fqdn": "wn6-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 4,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 7,
                "fqdn": "wn7-foobar",
                "state": "Succeeded",
                "faultDomain": 0,
                "updateDomain": 0,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/workernode-0"
            },
            {
                "vmId": 8,
                "fqdn": "wn8-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 0,
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
            },
            {
                "vmId": 1,
                "fqdn": "zk1-foobar",
                "state": "Succeeded",
                "faultDomain": 2,
                "updateDomain": 2,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/zookeepernode-0"
            },
            {
                "vmId": 2,
                "fqdn": "zk2-foobar",
                "state": "Succeeded",
                "faultDomain": 1,
                "updateDomain": 1,
                "availabilitySetId": "/subscriptions/63b34899-ad65-4508-bd67-d8d7efc28194/resourcegroups/rg0-d93ef9c75f8b46f6be278cc0fff8470fresourcegroup/providers/Microsoft.Compute/availabilitySets/zookeepernode-0"
            }
        ]
    }
}
        '''
        r = rebalance.parse_topo_info(a)
        self.assertEqual([0,0], r[0])
        self.assertEqual([1,1], r[1])
    
    '''
    Generate topology info along the diagonal of the two dimensions
    '''
    def _gen_topo_info_1(self, x_len, y_len, x, y, num):
        topo_info = [[x,y]]
        if x_len != y_len:
            for i in range(1, num):
                x = (x+1)%x_len
                y = (y+1)%y_len
                topo_info.append([x,y])
            return topo_info
        raise Exception("Cannot generate topology when x_len=y_len")
    
    '''
    Generate topology info row by row on the two dimensions
    '''
    def _gen_topo_info_2(self, x_len, y_len, x, y, num):
        topo_info = [[x,y]]
        for i in range(1, num):
            if x < x_len-1:
                x += 1
            else:
                x = 0
                y = (y+1)%y_len
            topo_info.append([x,y])
        return topo_info
    
    def test_reassign_basic(self):
        topo_info = self._gen_topo_info_1(3,5,0,2,16) # 16 node cluster
        print topo_info
        partitions_info = [[3,9,0], [0,10,5], [5,1,12]]
        rgen = rebalance.ReassignmentGenerator(topo_info, "test.1", partitions_info)
        r = rgen.reassign()
        self.assertEqual(2, len(r["partitions"]))
        print json.dumps(r)
        #make sure the reassigned replica does not have conflict
        rgen2 = rebalance.ReassignmentGenerator(topo_info, "test.1",
            [r["partitions"][0]["replicas"], r["partitions"][1]["replicas"], [5,1,12]])
        r2 = rgen2.reassign()
        self.assertEqual(None, r2)
        
    def test_reassign_extensive(self):
        topo_info = self._gen_topo_info_1(3,5,0,2,45) # 45 node cluster
        #random assign partitions
        partitions_info = [random.sample([i for i in range(45)], 3) for j in range(450)] #450 partitions
        rgen = rebalance.ReassignmentGenerator(topo_info, "test.extensive", partitions_info)
        r = rgen.reassign()
        #make sure the load is evenly distributed
        for load in rgen.broker_load:
            self.assertTrue(load > 25 and load < 35)
        
        #self.assertEqual(2, len(r["partitions"]))
        #print r
        #make sure the reassigned replica does not have conflict
        rgen2 = rebalance.ReassignmentGenerator(topo_info, "test.extensive",
            [r["partitions"][i]["replicas"] for i in range(len(r["partitions"]))])
        r2 = rgen2.reassign()
        self.assertEqual(None, r2)
        
    def test_no_conflict(self):
        topo_info = self._gen_topo_info_2(3,3,0,0,9) # 9 node cluster
        print topo_info
        partitions_info=[[0,4,8], [1,5,6], [0,5,7], [2,4,6], [1,3,8], [0,4,8], [2,3,7], [1,5,6]]
        rgen = rebalance.ReassignmentGenerator(topo_info, "test.no_conflict", partitions_info)
        self.assertEqual(None, rgen.reassign())

if __name__ == "__main__":
    unittest.main()