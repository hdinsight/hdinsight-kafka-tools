import json, logging, os, sys, time, traceback
from hdinsight_kafka.kafka_remote_storage import KafkaRemoteStorage

from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)
debug = False

brokers_ids_path = 'brokers/ids'
kafka_map_path = 'KafkaMap'
zf_pad = 2

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

    kfr = KafkaRemoteStorage()
    zk = kfr.connect(zookeepers)

    kase_brokers_ids = zk.get_children(kafka_map_path)

    kase_broker_hosts = {}
    for kase_broker_id in kase_brokers_ids:
        kase_broker_id_host, stat = zk.get('{0}/{1}/host'.format(kafka_map_path, kase_broker_id))
        kase_broker_id_storage, stat = zk.get('{0}/{1}/storage'.format(kafka_map_path, kase_broker_id))
        kase_broker_hosts[kase_broker_id] = '{0}, {1}'.format(kase_broker_id_host, kase_broker_id_storage.replace('.blob.','.file.'))

    logger.info('KASE expected brokers: {0}\n{1}\n'.
        format(len(kase_broker_hosts), 
            reduce(lambda b1, b2 : b1 + '\n' + b2, 
                sorted(
                    map(lambda b : b.zfill(zf_pad) + ' = ' + kase_broker_hosts[b], kase_broker_hosts)
                )
            )
        )
    )

    zk_brokers_ids = zk.get_children(brokers_ids_path)

    zk_broker_hosts = {}
    for zk_broker_id in zk_brokers_ids:
        zk_broker_id_data, stat = zk.get('{0}/{1}'.format(brokers_ids_path, zk_broker_id))
        zk_broker_info = json.loads(zk_broker_id_data)
        zk_broker_host = zk_broker_info['host']
        zk_broker_hosts[zk_broker_id] = zk_broker_host

    if len(zk_broker_hosts) > 1:
        logger.info('Zookeeper registered brokers: {0}\n{1}\n'.
            format(len(zk_broker_hosts), 
                reduce(lambda b1, b2 : b1 + '\n' + b2, 
                    sorted(
                        map(lambda b : b.zfill(zf_pad) + ' = ' + zk_broker_hosts[b], zk_broker_hosts)
                    )
                )
            )
        )
    else:
        logger.error('There are NO brokers registered in Zookeeper')

    dead_broker_hosts = {}
    for expected_broker_id in kase_broker_hosts:
        if not expected_broker_id in zk_broker_hosts:
            dead_broker_hosts[expected_broker_id] = kase_broker_hosts[expected_broker_id]

    if dead_broker_hosts:
        logger.info('Dead/Unregistered brokers: {0}\n{1}\n'.
            format(len(dead_broker_hosts), 
                reduce(lambda b1, b2 : b1 + '\n' + b2, 
                    sorted(
                        map(lambda b : b.zfill(zf_pad) + ' = ' + dead_broker_hosts[b], dead_broker_hosts)
                    )
                )
            )
        )

    if len(sys.argv) > 1:
        logger.info('\n==================================\nGetting Kafka pid statuses\n==================================\n')
        for kase_broker_host in kase_broker_hosts:
            broker_host = kase_broker_hosts[kase_broker_host].split(',', 1)[0]
            logger.info('\nBroker host: {0}\n-----------------------------------'.format(broker_host))
            try:
                cmd ='python -'
                #cmd ='sudo ps -f \$(cat /var/run/kafka/kafka.pid)'
                stdout, stderr = ktu.runShellCommand('cat get_kafkapidstatus.py | {0}ssh {1} -o StrictHostKeyChecking=no {2}@{3} "{4}"'
                                                     .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, cmd))
            except:
                logger.error(traceback.print_exc())
                logger.error('Failed to get kafka pid status.'.format(broker_host))
                logger.info('\n---------------------------------\n'.format(broker_host))
        if len(dead_broker_hosts) > 0:
            logger.info('\n==================================\nGetting dead brokers reasons\n==================================\n')
            for dead_broker_host in dead_broker_hosts:
                broker_host = dead_broker_hosts[dead_broker_host].split(',', 1)[0]
                logger.info('\nDead broker host: {0}\n-----------------------------------'.format(broker_host))
                try:
                    cmd ='sudo tail -n 30 /var/log/kafka/kafka.out'
                    stdout, stderr = ktu.runShellCommand('{0}ssh {1} -o StrictHostKeyChecking=no {2}@{3} "{4}"'
                                                         .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, cmd))
                except:
                    logger.error(traceback.print_exc())
                    logger.error('Failed to get kafka out messages.'.format(broker_host))
                    logger.info('\n---------------------------------\n'.format(broker_host))
        logger.info('\n==================================\n'.format(broker_host))

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, "brokercheck{0}.log".format(int(time.time())), debug)
    main(ktu)
