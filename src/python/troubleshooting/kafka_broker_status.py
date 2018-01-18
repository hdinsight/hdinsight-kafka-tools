"""Script to report the current Kafka Broker status for this HDInsight Kafka cluster.
"""
import logging, pprint, time

from kafka_utils import KafkaUtils

logger = logging.getLogger(__name__)
debug = False

def str_kafka_brokers_status(broker_hosts, zk_brokers):
    """Returns the formatted Broker status as string. Takes Broker Hosts from Ambari and Zookeeper as the input.
    """
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

def str_kafka_controller_status(zk_controller):
    """Returns the formatted Kafka Controller status as string. Takes Controller data from Zookeeper as the input.
    """
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

def get_kafka_broker_status(utils):
    """Gets Kafka Broker status by querying Ambari and Zookeeper.
    
    Returns Broker Hosts in Ambari and Zookeeper, and Dead Brokers. All the returned objects are dictionaries of dictionaries with host name as the key.
    """
    broker_hosts, brokers = utils.get_brokers_from_ambari()
    logger.info('Ambari brokers: {0}\n{1}\n'.format(len(broker_hosts), pprint.pformat(broker_hosts)))

    zk_brokers = utils.get_brokers_from_zookeeper()
    logger.info(str_kafka_brokers_status(broker_hosts, zk_brokers))

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

def get_kafka_controller_status(utils, broker_hosts, zk_brokers):
    """Gets Kafka Controller status by querying Zookeeper. Returns a dictionary object for the controller.
    
    Returned object contains following keys:
    controller_id (broker_id)
    controller_host
    controller_ip
    """
    zk_controller = utils.get_controller_from_zookeeper()
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
            logger.info(str_kafka_controller_status(zk_controller))
    else:
        err_msg = 'There are no brokers or controller online.'
        logger.error(err_msg)
        raise RuntimeError(err_msg)
    return zk_controller

def main(utils):
    broker_hosts, zk_brokers, dead_broker_hosts = get_kafka_broker_status(utils)
    zk_controller = get_kafka_controller_status(utils, broker_hosts, zk_brokers)

if __name__ == '__main__':
    utils = KafkaUtils(logger, "kafkabrokerstatus{0}.log".format(int(time.time())), debug)
    main(utils)
