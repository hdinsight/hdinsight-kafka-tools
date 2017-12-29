import logging, sys, time
from kafka_utils import KafkaUtils

logger = logging.getLogger(__name__)

def main(utils):
    topic_name = ''
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    logger.info('topic_name = ' + topic_name)
    zookeeper_quorum = utils.get_zookeeper_quorum()
    logger.info('Listing topics')
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0}".format(zookeeper_quorum)
    if(topic_name):
        shell_command += " --topic {0}".format(topic_name)

    stdout, stderr = utils.run_shell_command(shell_command)

if __name__ == '__main__':
    utils = KafkaUtils(logger, "kafkatopicdescribe{0}.log".format(int(time.time())))
    main(utils)
