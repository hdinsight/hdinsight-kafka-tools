import logging, pprint, time

from kafka_utils import KafkaUtils
from kafka_broker_status import get_broker_status, print_zookeeper_brokers
from kafka_broker_status import get_controller_status, print_zookeeper_controller

logger = logging.getLogger(__name__)
debug = False

def main(utils):
    broker_hosts, zk_brokers, dead_broker_hosts = get_broker_status(utils)
    zk_controller = get_controller_status(utils, broker_hosts, zk_brokers)
    logger.info('Restarting Kafka Controller at broker id {0} on host {1}'.format(zk_controller['controller_id'], zk_controller['controller_host']))
    response = utils.restart_broker(zk_controller['controller_host'])
    logger.debug(pprint.pformat(response))
    logger.info('Successfully restarted Kafka Controller at broker id {0} on host {1}'.format(zk_controller['controller_id'], zk_controller['controller_host']))
    zk_controller = get_controller_status(utils, broker_hosts, zk_brokers)
    logger.info('\nNEW Kafka Controller is at broker id {0} on host {1}'.format(zk_controller['controller_id'], zk_controller['controller_host']))
    logger.info(print_zookeeper_controller(zk_controller))

if __name__ == '__main__':
    utils = KafkaUtils(logger, "kafkarestartcontroller{0}.log".format(int(time.time())), debug)
    main(utils)
