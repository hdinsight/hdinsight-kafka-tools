# HDInsight Kafka Tools

This repository contains scripts, tools and ARM templates for Apache Kafka on HDInsight clusters.
Please refer to the table below for different resources in this repository.

| Location                                            | Description                                                                                                                                           |
|-----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| [ARM Templates](src/arm/)                           | Azure ARM templates for HDInsight Kafka 2.1 and 2.4 and various other cluster types that can be deployed in the same VNET along with HDInsight Kafka. |
| [Powershell](src/powershell)                        | Powershell scripts to create HDInsight Kafka 2.1 and 2.4 clusters.                                                                                    |
| [Kafka Rebalance](src/python/rebalance)             | Python script to rebalance (re-assign) Kafka Topics and Partitions across different Azure Fault Domains and Upgrade Domains for high availability.    |
| [Kafka Troubleshooting](src/python/troubleshooting) | Python scripts to check the status of Kafka brokers and restart brokers based on their health.                                                        |

## Other HDInsight Kafka Resources
* [HDInsight Kafka - Getting Started](https://docs.microsoft.com/en-us/azure/hdinsight/kafka/apache-kafka-get-started)
* [HDInsight FAQs](https://hdinsight.github.io/)
* [HDInsight Kafka FAQs](https://hdinsight.github.io/kafka/kafka-landing)

For feedback, please open new issues or write to us at hdikafka at microsoft dot com.