import logging, os, sys, time, traceback
from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)
debug = False

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
    errored_brokers = []
    for broker_host in broker_hosts:
        if broker_host:
            logger.info('\nPatching broker host: {0}\n-----------------------------------'.format(broker_host))
            try:
                cmd = 'sudo chage -I -1 -m 0 -M 99999 -E -1 {2}'
                stdout, stderr = ktu.runShellCommand('{0}ssh {1} -o StrictHostKeyChecking=no {2}@{3} "{4}"'
                                                     .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, cmd))
            except:
                logger.error(traceback.print_exc())
                errored_brokers.append(broker_host)
                #raise RuntimeError('Failing due to an execution error')
                logger.info('\nBroker host: {0} patching failed\n==================================\n'.format(broker_host))

    if len(errored_brokers) > 0:
        logger.info('Errored brokers: ' + str(len(errored_brokers)) + '\n' + reduce(lambda x, y : x + y, map(lambda b : b + '\n', errored_brokers)))

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, 'updatepasswordexpiry{0}'.format(int(time.time())) + '.log')
    main(ktu)
