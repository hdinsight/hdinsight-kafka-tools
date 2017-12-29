import argparse, logging, pprint, sys, time

from kafka_utils import KafkaUtils
from kafka_broker_status import get_broker_status, print_zookeeper_brokers

logger = logging.getLogger(__name__)
debug = False

TIMEOUT_SECS = 3600
WAIT_SECS = 300
SLEEP_SECS = 30

def main(args, utils):
    force = args.force
    all = args.all
    if all:
        force = True

    broker_hosts, zk_brokers, dead_broker_hosts = get_broker_status(utils)
    if len(dead_broker_hosts) > 0:
        logger.warn('Dead/Unregistered brokers: {0}\n{1}\n'.
            format(len(dead_broker_hosts), pprint.pformat(dead_broker_hosts)))
        if not force:
            err_msg = 'One or more brokers are not online, cannot proceed with broker restarts.'
            logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            logger.info('Forcing restart of brokers inspite of dead or unregistered brokers.')
    else:
        logger.info('All brokers are online and registered in zookeeper, proceeding with restarts of stale broker hosts unless all options is specified.')

    if all:
        logger.info('Restarting all brokers irrespective of stale configs or dead brokers.')
        restart_broker_hosts = broker_hosts.keys()
    else:
        restart_broker_hosts = utils.get_stale_broker_hosts()

    if len(restart_broker_hosts) == 0:
        logger.debug('No brokers found to restart.')
    else:
        logger.info('Restarting brokers on following hosts: {0}\n{1}\n'.format(len(restart_broker_hosts), pprint.pformat(restart_broker_hosts)))

    for restart_broker_host in restart_broker_hosts:
        zk_brokers = utils.get_zookeeper_brokers()
        if(restart_broker_host not in zk_brokers) and (not force):
            err_msg = 'Broker host {0} is not online, cannot proceed with restarts.'.format(restart_broker_host)
            logger.error(err_msg)
            raise RuntimeError(err_msg)
        else:
            logger.info('Restarting Kafka Broker on {0}'.format(restart_broker_host))
            utils.restart_broker(restart_broker_host)
            zk_brokers = utils.get_zookeeper_brokers()
            now = time.time()
            timeout = now + TIMEOUT_SECS
            while(restart_broker_host not in zk_brokers):
                if time.time() > timeout:
                    err_msg = 'Kafka Broker on {0} failed to come online in Zookeeper in {1} secs. Please check the Kafka server log for potential issues'.format(restart_broker_host, TIMEOUT_SECS)
                    logger.error(err_msg)
                    raise RuntimeError(err_msg)
                logger.info('Kafka Broker on {0} is not yet online in Zookeper. Sleeping for {1} seconds...'.format(restart_broker_host, SLEEP_SECS))
                time.sleep(SLEEP_SECS)
                zk_brokers = utils.get_zookeeper_brokers()
        logger.info('Kafka Broker on {0} sucessfully restarted and is now online in Zookeeper. Waiting for {1} seconds before proceeding to the next broker.'.format(restart_broker_host, WAIT_SECS))
        time.sleep(WAIT_SECS)

    logger.info('All brokers with stale configs have been restarted.')
    restart_broker_hosts = utils.get_stale_broker_hosts()
    zk_brokers = utils.get_zookeeper_brokers()
    logger.info(print_zookeeper_brokers(broker_hosts, zk_brokers))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Restart Kafka Brokers one by one and wait for their Zookeeper state to be alive.')
    parser.add_argument('-f', '--force', action='store_true', help='Restart brokers with stale configs irrespective of their current health.')
    parser.add_argument('-a', '--all', action='store_true', help='Restart all brokers one by one irrespective of their current health.')
    args = parser.parse_args()
    utils = KafkaUtils(logger, 'kafkarestartbrokers{0}.log'.format(int(time.time())), debug)
    main(args, utils)
