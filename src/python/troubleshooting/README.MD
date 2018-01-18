# HDInsight Kafka Troubleshooting Python Scripts

Repository of Python scripts that can be executed on any HDInsight Kafka cluster node via SSH. 
Each of these scripts provides a support action which you can use to either get the health status of Kafka or perform certain actions like restarting Brokers and waiting for them to be healthy before proceeding to next one.

The scripts use Ambari and Zookeeper APIs to gather the information and execute appropriate Ambari Actions using internal libraries, such that users do not need to provide any host names or credentials.
Each script also generates a local log file whose name is also displayed on the console that can be shared with Microsoft support for further help/investigations.

The scripts require HDInsight Clusters to be of versions HDI 3.5 (HDP 2.5/Kafka 0.9), HDI 3.6 (HDP 2.6/Kafka 0.10) or above.

## How to use this repository
Clone the repository on any HDInsight cluster node using the following command:

```
git clone https://github.com/hdinsight/hdinsight-kafka-tools.git
```

The scripts are be located under ```hdinsight-kafka-tools/src/python/troubleshooting``` and can be executed from any directory.

|Script|File|When to use|Description|
-------|----|-----------|-----------|
|Kafka Performance Test|[kafka_perf_test.py](kafka_perf_test.py)|Anytime|A quick performance test for Kafka.|
|Kafka Broker Status|[kafka_broker_status.py](kafka_broker_status.py)|Anytime|Reports Broker status for all Kafka Brokers.|
|Kafka Restart Brokers|[kafka_restart_brokers.py](kafka_restart_brokers.py)|Post Kafka configuration update|Restarts Kafka brokers based on health check.|
|Kafka Restart Controller|[kafka_restart_controller.py](kafka_restart_controller.py)|For clearing Kafka controller state for partition leadership issues|Restarts Kafka Broker that is the current controller.|
|Kafka Topic Describe|[kafka_topic_describe.py](kafka_topic_describe.py)|Anytime|Wrapper script for Kafka Topic describe that takes care of all inputs.|


## Kafka Performance Test
A simple script that uses Kafka Performance tools to create a topic, produce and consume data from the topic and then deletes the topic. All the parameters are taken care of by the script.
The number of partitions will be equal to the number of brokers in the system and the script will send 1 million messages of 100 bytes each per partition and report the numbers for each of the steps.

```bash
sudo python kafka_perf_test.py
```

## Kafka Broker Status
Handy script to list the uptime of all Kafka Brokers and also provide a list of hosts where Kafka Broker is down and not registered with the Zookeeper.
The script also provides the broker id to host name and ip mapping.

```bash
sudo python kafka_broker_status.py
```

Sample output:
```
2017-12-29 04:55:00,514 - kafka_broker_status.py - __main__ - INFO - Zookeeper registered brokers: 3
broker.id        broker.host                                                        broker.ip            broker.timestamp               broker.uptime
1001             wn1-kafkar.averylargefqdnincloudexist.cx.internal.cloudapp.net     10.0.0.32            2017-12-29 01:20:00            3:35:00.440146
1002             wn0-kafkar.averylargefqdnincloudexist.cx.internal.cloudapp.net     10.0.0.33            2017-12-29 02:11:53            2:43:07.402321
1003             wn2-kafkar.averylargefqdnincloudexist.cx.internal.cloudapp.net     10.0.0.36            2017-12-29 01:21:37            3:33:23.469578

2017-12-29 04:55:00,514 - kafka_broker_status.py - __main__ - INFO - There are no dead or unregistered brokers
2017-12-29 04:55:00,595 - kafka_broker_status.py - __main__ - INFO - Zookeeper registered controller:
controller.id    controller.host                                                        controller.ip        controller.timestamp           controller.uptime
1001             wn1-kafkar.averylargefqdnincloudexist.cx.internal.cloudapp.net         10.0.0.32            2017-12-29 02:11:31            2:43:28.751835
```

## Kafka Restart Brokers
Restart Kafka Brokers that have stale configs i.e. brokers are pending restart in Ambari after configuration update. 
The script queries for brokers with stale configs from Ambari and then checks if all the brokers are alive in Zookeeper and then initiates restarts one by one.
Post each restart of the Kafka Broker component on each host, it waits for the Kafka server startup to complete and check if the broker is registered in Zookeeper before proceeding to the next one.

```bash
sudo python kafka_restart_brokers.py
```

Two additional options are available in the script to force restart of all or only stale brokers irrespective of their Zookeeper state.

```
usage: kafka_restart_brokers.py [-h] [-f] [-a]

Restart Kafka Brokers one by one and wait for their Zookeeper state to be alive.

optional arguments:
  -h, --help   show this help message and exit
  -f, --force  Restart brokers with stale configs irrespective of their current health.
  -a, --all    Restart all brokers one by one irrespective of their current health.
```

## Kafka Restart Controller
Restart the Kafka Broker which is the current acting controller. 
The script checks the broker and controller information in the Zookeeper and then issues the restart of Kafka Broker via Ambari for the host that maps to that broker id.

```bash
sudo python kafka_restart_controller.py
```

## Kafka Topic Describe
A simple script that describes all or some topics on the Kafka cluster.

```bash
sudo python kafka_topic_describe.py
```

## Kafka Get PID Status
A simple script to report the detailed status of the Kafka process especially if the process became a zombie. Helpful in cases where it is not responding to any actions and Ambari sees it as healthy.
This script has to be run on the node where the Kafka process is running.

```bash
sudo python kafka_get_pid_status.py
```

## Run Custom Commands
This is a DIY patch the cluster script, where you can run any command or copy files across all Kafka Broker hosts by providing SSH creds. 
Some use cases include extending the password expiry for the current SSH user or checking the status of certain process.

```bash
sudo python run_custom_commands.py
```

```
usage: run_custom_commands.py [-h] ssh_username ssh_password

Run custom commands on all Kafka Brokers hosts.

positional arguments:
  ssh_username  The SSH User name for the nodes.
  ssh_password  The SSH Password or SSH Key File for the SSH User.

optional arguments:
  -h, --help    show this help message and exit
```