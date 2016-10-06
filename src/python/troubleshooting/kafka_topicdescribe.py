import logging, sys, time
from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)

def main(ktu):
    topic_name = ''
    if len(sys.argv) > 1:
        topic_name = sys.argv[1]
    logger.info('topic_name = ' + topic_name)
    zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize = ktu.getShellInputs()
    logger.info("Listing topics")
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0}".format(zookeepers)
    if(topic_name):
        shell_command += " --topic {0}".format(topic_name)

    stdout, stderr = ktu.runShellCommand(shell_command)

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, "kafkatopicdescribe{0}.log".format(int(time.time())))
    main(ktu)
