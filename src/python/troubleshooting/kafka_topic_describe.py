import logging, sys, time
from kafka_utils import KafkaUtils

logger = logging.getLogger(__name__)


def main(utils):
    topic_name = ''
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    logger.info('topic_name = ' + topic_name)
    logger.info('Listing topics')
    kafka_version, hdi_version = utils.get_kafka_hdp_version()
    if kafka_version >= '3.2.0':
        (_, broker_server) = utils.get_brokers_from_ambari()
        logger.info(broker_server)
        shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --bootstrap-server {0}".format(
            broker_server)
    else:
        zookeeper_quorum = utils.get_zookeeper_quorum()
        shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0}".format(
            zookeeper_quorum)
    if topic_name:
        shell_command += " --topic {0}".format(topic_name)

    stdout, stderr = utils.run_shell_command(shell_command)


if __name__ == '__main__':
    utils = KafkaUtils(logger, "kafkatopicdescribe{0}.log".format(int(time.time())))
    main(utils)
