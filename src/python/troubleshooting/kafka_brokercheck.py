import datetime, json, logging, os, sys, time, traceback

from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)
debug = False

brokers_ids_path = 'brokers/ids'
kafka_getpidstatus_script = os.path.dirname(os.path.realpath(__file__)) + os.sep + 'kafka_getpidstatus.py'

def main(ktu):
    ssh_username = ''
    if len(sys.argv) > 1:
        ssh_username = sys.argv[1]
    logger.info('ssh_username = ' + ssh_username)

    script_dir = os.path.dirname(__file__)

    ssh_key_param = ''
    ssh_password_param = ''

    ssh_password = ''
    ssh_key_file = ''
    if len(sys.argv) > 2:
        ssh_password = sys.argv[2]

    if ssh_password:
        if os.path.exists(ssh_password):
            ssh_key_file = ssh_password
        elif os.path.exists(os.path.join(script_dir, ssh_password)):
            ssh_key_file = os.path.join(script_dir, ssh_password)

    if os.path.exists(ssh_key_file):
        logger.info('ssh_key_file = ' + ssh_key_file)
        stdout, stderr = ktu.runShellCommand('chmod 600 {0}'.format(ssh_key_file))
        ssh_key_param = '-i {0}'.format(ssh_key_file)
    else:
        logger.info('ssh_password = ' + ssh_password)
        ssh_password_param = 'sshpass -p {0} '.format(ssh_password)
        stdout, stderr = ktu.runShellCommand('sudo apt-get install sshpass')


    zookeepers, broker_hosts, brokers = ktu.getBrokerInformation()
    logger.info('Ambari brokers:{0}\n{1}\n'.
        format(len(broker_hosts), 
            reduce(lambda b1, b2 : b1 + '\n' + b2, sorted(broker_hosts))
        )
    )

    zk = ktu.connect(zookeepers)

    zk_brokers_ids = zk.get_children(brokers_ids_path)

    zk_broker_hosts = {}
    for zk_broker_id in zk_brokers_ids:
        zk_broker_id_data, stat = zk.get('{0}/{1}'.format(brokers_ids_path, zk_broker_id))
        zk_broker_info = json.loads(zk_broker_id_data)
        zk_broker_info['id'] = zk_broker_id
        zk_broker_datetime = datetime.datetime.utcfromtimestamp(int(zk_broker_info['timestamp'])/1000.0)
        zk_broker_info['datetime'] = zk_broker_datetime.strftime('%Y-%m-%d %H:%M:%S')
        zk_broker_duration = str((datetime.datetime.utcnow() - zk_broker_datetime))
        zk_broker_info['duration'] = zk_broker_duration
        zk_broker_hosts[zk_broker_info['host']] = zk_broker_info

    if len(zk_broker_hosts) > 0:
        logger.info('Zookeeper registered brokers: {0}\n{1}\n{2}\n'.
            format(
                len(zk_broker_hosts), 
                'broker.id'.ljust(10) + '\t' + 'broker.host'.ljust(64) + '\t' + 'broker.timestamp'.ljust(20) + '\t' + 'broker.uptime'.ljust(30),
                reduce(lambda b1, b2 : b1 + '\n' + b2, 
                    sorted(
                        map(lambda b : str(zk_broker_hosts[b]['id']).ljust(10) + '\t' + b.ljust(64) +  '\t' + 
                        zk_broker_hosts[b]['datetime'].ljust(20) + '\t' + zk_broker_hosts[b]['duration'].ljust(30), 
                        zk_broker_hosts
                        )
                    )
                )
            )
        )
    else:
        logger.error('There are NO brokers registered in Zookeeper')

    dead_broker_hosts = {}
    for expected_broker_host in broker_hosts:
        if not expected_broker_host in zk_broker_hosts:
            dead_broker_hosts[expected_broker_host] = expected_broker_host

    if len(dead_broker_hosts) > 0:
        logger.info('Dead/Unregistered brokers: {0}\n{1}\n'.
            format(len(dead_broker_hosts), 
                reduce(lambda b1, b2 : b1 + '\n' + b2, 
                    sorted(
                        map(lambda b : dead_broker_hosts[b], dead_broker_hosts)
                    )
                )
            )
        )
    else:
        logger.info('There are no dead or unregistered brokers')

    if len(sys.argv) > 1:
        logger.info('\n==================================\nGetting Kafka pid statuses\n==================================\n')
        for broker_host in broker_hosts:
            logger.info('\nBroker host: {0}\n-----------------------------------'.format(broker_host))
            try:
                cmd ='python -'
                stdout, stderr = ktu.runShellCommand('cat ' + kafka_getpidstatus_script + ' | {0}ssh {1} -o StrictHostKeyChecking=no {2}@{3} "{4}"'
                                                     .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, cmd))
            except:
                logger.error(traceback.print_exc())
                logger.error('Failed to get kafka pid status.'.format(broker_host))
                logger.info('\n---------------------------------\n'.format(broker_host))
        if len(dead_broker_hosts) > 0:
            logger.info('\n==================================\nGetting dead brokers reasons\n==================================\n')
            for dead_broker_host in dead_broker_hosts:
                broker_host = dead_broker_hosts[dead_broker_host]
                logger.info('\nDead broker host: {0}\n-----------------------------------'.format(broker_host))
                try:
                    cmd ='sudo tail -n 30 /var/log/kafka/server.log'
                    stdout, stderr = ktu.runShellCommand('{0}ssh {1} -o StrictHostKeyChecking=no {2}@{3} "{4}"'
                                                         .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, cmd))
                except:
                    logger.error(traceback.print_exc())
                    logger.error('Failed to get kafka server log messages.'.format(broker_host))
                    logger.info('\n---------------------------------\n'.format(broker_host))
        logger.info('\n==================================\n'.format(broker_host))

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, "kafkabrokercheck{0}.log".format(int(time.time())), debug)
    main(ktu)
