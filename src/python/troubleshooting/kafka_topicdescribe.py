import logging, time
from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)

def main(ktu):
    zookeepers, brokers, partitions, replicationfactor, messages, threads, messagesize, batchsize = ktu.getShellInputs()
    logger.info("Listing topics")
    shell_command = "/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper {0}".format(zookeepers)
    stdout, stderr = ktu.runShellCommand(shell_command)

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, "topicdescribe{0}.log".format(int(time.time())))
    main(ktu)
