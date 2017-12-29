import logging, pprint, time

from kafka_utils import KafkaUtils

logger = logging.getLogger(__name__)
debug = False

def print_zookeeper_brokers(broker_hosts, zk_brokers):
    if len(zk_brokers) > 0:
        return ('Zookeeper registered brokers: {0}\n{1}\n{2}\n'.
            format(
                len(zk_brokers), 
                '{0} {1} {2} {3} {4}'.format(
                    'broker.id'.ljust(16), 
                    'broker.host'.ljust(66), 
                    'broker.ip'.ljust(20), 
                    'broker.timestamp'.ljust(30), 
                    'broker.uptime'.ljust(30)),
                reduce(lambda b1, b2 : b1 + '\n' + b2, 
                    sorted(
                        map(lambda b : '{0} {1} {2} {3} {4}'.format(
                            str(zk_brokers[b]['id']).ljust(16), 
                            b.ljust(66), 
                            broker_hosts[b].ljust(20), 
                            zk_brokers[b]['datetime'].ljust(30), 
                            zk_brokers[b]['duration'].ljust(30)), 
                        zk_brokers
                        )
                    )
                )
            )
        )
    else:
        return 'There are NO brokers registered in Zookeeper.'

def print_zookeeper_controller(zk_controller):
    return ('Zookeeper registered controller: \n{0}\n{1}\n'.
                format(
                    '{0} {1} {2} {3} {4}'.format(
                        'controller.id'.ljust(16), 
                        'controller.host'.ljust(70) , 
                        'controller.ip'.ljust(20) , 
                        'controller.timestamp'.ljust(30), 
                        'controller.uptime'.ljust(30)),
                    '{0} {1} {2} {3} {4}'.format(
                        zk_controller['controller_id'].ljust(16), 
                        zk_controller['controller_host'].ljust(70), 
                        zk_controller['controller_ip'].ljust(20), 
                        zk_controller['datetime'].ljust(30), 
                        zk_controller['duration'].ljust(30))
                ))

def get_broker_status(utils):
    broker_hosts, brokers = utils.get_brokers()
    logger.info('Ambari brokers: {0}\n{1}\n'.format(len(broker_hosts), pprint.pformat(broker_hosts)))

    zk_brokers = utils.get_zookeeper_brokers()
    logger.info(print_zookeeper_brokers(broker_hosts, zk_brokers))

    dead_broker_hosts = {}
    for expected_broker_host in broker_hosts:
        if not expected_broker_host in zk_brokers:
            dead_broker_hosts[expected_broker_host] = broker_hosts[expected_broker_host]

    if len(dead_broker_hosts) > 0:
        logger.warn('Dead/Unregistered brokers: {0}\n{1}\n'.
            format(len(dead_broker_hosts), pprint.pformat(dead_broker_hosts)))
    else:
        logger.info('There are no dead or unregistered brokers')
    
    return broker_hosts, zk_brokers, dead_broker_hosts

def get_controller_status(utils, broker_hosts, zk_brokers):
    zk_controller = utils.get_controller()
    if zk_brokers and zk_controller:
        zk_controller['controller_id'] = str(zk_controller['brokerid'])
        for zk_broker_host, zk_broker_info in zk_brokers.iteritems():
            if zk_broker_info['id'] == zk_controller['controller_id']:
                zk_controller['controller_host'] = str(zk_broker_host)
                zk_controller['controller_ip'] = str(broker_hosts[zk_controller['controller_host']])
                break
        if 'controller_host' not in zk_controller:
            logger.error('Unable to find controller host information')
        else:
            logger.info(print_zookeeper_controller(zk_controller))
    else:
        err_msg = 'There are no brokers or controller online.'
        logger.error(err_msg)
        raise RuntimeError(err_msg)
    return zk_controller

def main(utils):
    broker_hosts, zk_brokers, dead_broker_hosts = get_broker_status(utils)
    zk_controller = get_controller_status(utils, broker_hosts, zk_brokers)

if __name__ == '__main__':
    utils = KafkaUtils(logger, "kafkabrokercheck{0}.log".format(int(time.time())), debug)
    main(utils)
