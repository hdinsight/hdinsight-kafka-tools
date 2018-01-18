"""Script to restart the current Kafka Broker which is acting as the Controller
"""
import logging, pprint, time

from kafka_utils import KafkaUtils
from kafka_broker_status import get_kafka_broker_status, get_kafka_controller_status, str_kafka_controller_status

logger = logging.getLogger(__name__)
debug = False

def main(utils):
    broker_hosts, zk_brokers, dead_broker_hosts = get_kafka_broker_status(utils)
    zk_controller = get_kafka_controller_status(utils, broker_hosts, zk_brokers)
    logger.info(str_kafka_controller_status(zk_controller))
    logger.info('Restarting Kafka Controller at broker id {0} on host {1}'.format(zk_controller['controller_id'], zk_controller['controller_host']))
    response = utils.restart_kafka_broker_from_ambari(zk_controller['controller_host'])
    logger.debug(pprint.pformat(response))
    logger.info('Successfully restarted Kafka Controller at broker id {0} on host {1}'.format(zk_controller['controller_id'], zk_controller['controller_host']))
    zk_controller = get_kafka_controller_status(utils, broker_hosts, zk_brokers)
    logger.info('\nNEW Kafka Controller is at broker id {0} on host {1}'.format(zk_controller['controller_id'], zk_controller['controller_host']))
    logger.info(str_kafka_controller_status(zk_controller))

if __name__ == '__main__':
    utils = KafkaUtils(logger, "kafkarestartcontroller{0}.log".format(int(time.time())), debug)
    main(utils)
