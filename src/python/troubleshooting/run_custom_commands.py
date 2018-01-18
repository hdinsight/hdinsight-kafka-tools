import argparse, logging, os, sys, time, traceback
from kafka_utils import KafkaUtils

logger = logging.getLogger(__name__)
debug = False

def copy_files(broker_host, ssh_password_param, ssh_key_param, ssh_username, src, dest):
    stdout, stderr = utils.run_shell_command('{0}scp {1} -o StrictHostKeyChecking=no {4} {2}@{3}:{5}'
                         .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, src, dest))
    return stdout, stderr

def run_command(broker_host, ssh_password_param, ssh_key_param, ssh_username, cmd):
    stdout, stderr = utils.run_shell_command('{0}ssh {1} -o StrictHostKeyChecking=no {2}@{3} "{4}"'
                         .format(ssh_password_param, ssh_key_param, ssh_username, broker_host, cmd))
    return stdout, stderr

def main(args, utils):
    script_dir = os.path.dirname(__file__)

    ssh_username = args.ssh_username
    logger.info('ssh_username = ' + ssh_username)

    ssh_key_param = ''
    ssh_password_param = ''

    ssh_password = args.ssh_password
    ssh_key_file = ''

    if os.path.exists(ssh_password):
        ssh_key_file = ssh_password
    elif os.path.exists(os.path.join(script_dir, ssh_password)):
        ssh_key_file = os.path.join(script_dir, ssh_password)

    if os.path.exists(ssh_key_file):
        logger.info('ssh_key_file = ' + ssh_key_file)
        stdout, stderr = utils.run_shell_command('chmod 600 {0}'.format(ssh_key_file))
        ssh_key_param = '-i {0}'.format(ssh_key_file)
    else:
        logger.debug('ssh_password = ' + ssh_password)
        ssh_password_param = 'sshpass -p {0} '.format(ssh_password)
        stdout, stderr = utils.run_shell_command('sudo apt-get install sshpass')

    broker_hosts, brokers = utils.get_brokers_from_ambari()
    errored_brokers = []
    for broker_host in broker_hosts:
        if broker_host:
            logger.info('\nPatching broker host: {0}\n-----------------------------------'.format(broker_host))
            try:
                stdout, stderr = copy_files(broker_host, ssh_password_param, ssh_key_param, ssh_username, '~/*.txt', '~/')
                #cmd = 'sudo dpkg -i ~/*.deb'
                #cmd = 'sudo chage -I -1 -m 0 -M 99999 -E -1 {2}'
                cmd = 'sudo ls -alFh ~/'
                stdout, stderr = run_command(broker_host, ssh_password_param, ssh_key_param, ssh_username, cmd)
                logger.info('\nBroker host: {0} successfully patched\n==================================\n'.format(broker_host))
            except:
                logger.error(traceback.print_exc())
                errored_brokers.append(broker_host)
                #raise RuntimeError('Failing due to an execution error')
                logger.info('\nBroker host: {0} patching failed\n==================================\n'.format(broker_host))

    if len(errored_brokers) > 0:
        logger.info('Errored brokers that were not be patched: {0}\n{1}\n'.format(len(errored_brokers), reduce(lambda x, y : x + y, map(lambda b : b + '\n', errored_brokers))))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run custom commands on all Kafka Brokers hosts.')
    parser.add_argument('ssh_username', help='The SSH Username for the nodes.')
    parser.add_argument('ssh_password', help='The SSH Password or SSH Key File for the SSH User.')
    args = parser.parse_args()
    utils = KafkaUtils(logger, 'runcustomcommands{0}'.format(int(time.time())) + '.log')
    main(args, utils)
