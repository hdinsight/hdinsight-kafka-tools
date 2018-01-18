import datetime, json, logging, pprint, subprocess, time, sys
from retry import retry

from hdinsight_common.AmbariHelper import AmbariHelper
from hdinsight_common import Constants

from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.protocol.states import EventType

class KafkaUtils:
    """Utility class for Kafka troubleshooting scripts.
    
    The class provides methods and routines for various Ambari and Zookeeper operations.
    """
    
    """Constants"""
    SERVICE_KAFKA = 'KAFKA'
    COMPONENT_KAFKA_BROKER = 'KAFKA_BROKER'
    ZK_KAFKA_BROKER_PATH = 'brokers/ids'
    ZK_KAFKA_CONTROLLER_PATH = 'controller'
    STATE_INSTALLED = 'INSTALLED'
    STATE_STARTED = 'STARTED'

    KAFKA_BROKER_PORT = '9092'
    ZOOKEEPER_PORT = '2181'

    TIMEOUT_SECS = 900
    SLEEP_SECS = 30

    def __init__(self, logger_arg, log_file, debug_mode = False):
        """Constructor for KafkaUtils that sets up the logging module and AmbariHelper.
        """
        self.debug_mode = debug_mode
        self.logger = logger_arg
        self.logger.setLevel(logging.DEBUG)

        logging_format = '%(asctime)s - %(filename)s - %(name)s - %(levelname)s - %(message)s'
        # create console handler and set level to info
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # create file handler and set level to debug
        handler = logging.FileHandler(log_file, 'a')
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(logging_format)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.logger.info('Log file: {0}'.format(log_file))

        self.ambari_helper = AmbariHelper()
        self.cluster_manifest = self.ambari_helper.get_cluster_manifest()
        self.cluster_name = self.cluster_manifest.deployment.cluster_name

    def run_shell_command(self, shell_command, throw_on_error = True):
        """Runs a shell command locally and returns the stdout, stderr
        """
        self.logger.info(shell_command)
        if not self.debug_mode:
            p = subprocess.Popen(shell_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
            stdout, stderr = p.communicate()

            if stdout:
                self.logger.info('\n' + stdout)

            if stderr:
                self.logger.error('\n' + stderr)
        
            if throw_on_error and (not (p.returncode == 0)):
                self.logger.error('Process returned non-zero exit code: {0}'.format(p.returncode))
                sys.exit(p.returncode)

            return stdout, stderr

    def get_hosts_from_ambari(self):
        """Gets Hosts from Ambari.
        """
        hosts_result = self.ambari_helper.request_url('clusters/{0}/hosts'.format(self.cluster_name), 'GET', {'fields' : 'Hosts/ip'})

        hosts = {}
        for host in hosts_result['items']:
            hosts[host['Hosts']['host_name']] = host['Hosts']['ip']
        self.logger.debug('hosts: {0}\n'.format(pprint.pformat(hosts)))
        return hosts

    def get_zookeeper_hosts(self):
        """Gets Zookeeper hosts from Ambari.
        """
        hosts = self.get_hosts_from_ambari()

        zk_hosts = { k:v for k,v in hosts.iteritems() if k.startswith(self.cluster_manifest.settings[Constants.ZOOKEEPER_VM_NAME_PREFIX_SETTING_KEY])}
        self.logger.debug('zk_hosts:\n{0}\n'.format(pprint.pformat(zk_hosts)))

        zk_quorum = reduce(lambda r1, r2 : r1 + ',' + r2,
                        map(lambda m : m + ':' + self.ZOOKEEPER_PORT, zk_hosts))

        self.logger.debug('zk_quorum: {0}\n'.format(zk_quorum))

        return zk_hosts, zk_quorum

    def get_zookeeper_quorum(self):
        """Gets the Zookeeper quorum.
        """
        zk_hosts, zk_quorum = self.get_zookeeper_hosts()
        return zk_quorum

    def get_brokers_from_ambari(self):
        """Gets Broker Information. Returns Dictionary of Brokers hosts and the Broker Connection String.
        """
        hosts = self.get_hosts_from_ambari()

        broker_hosts = { k:v for k,v in hosts.iteritems() if k.startswith(self.cluster_manifest.settings[Constants.WORKERNODE_VM_NAME_PREFIX_SETTING_KEY])}
        self.logger.debug('broker_hosts:\n{0}\n'.format(pprint.pformat(broker_hosts)))

        brokers = reduce(lambda r1, r2 : r1 + ',' + r2,
                      map(lambda m : m + ':' + self.KAFKA_BROKER_PORT,
                          broker_hosts))
        self.logger.debug('brokers: {0}\n'.format(brokers))

        return broker_hosts, brokers

    def get_brokers_from_zookeeper(self):
        """Gets the Kafka Broker information from Zookeeper for live brokers.
        
        Returns a dictionary of Brokers with host name as the key, where each entry contains another dictionary for that broker information
        """
        zk_quorum = self.get_zookeeper_quorum()
        zk = self.zk_connect(zk_quorum)

        zk_brokers_ids = zk.get_children(self.ZK_KAFKA_BROKER_PATH)

        zk_brokers_info = {}
        for zk_broker_id in zk_brokers_ids:
            zk_broker_id_data, stat = zk.get('{0}/{1}'.format(self.ZK_KAFKA_BROKER_PATH, zk_broker_id))
            zk_broker_info = json.loads(zk_broker_id_data)
            zk_broker_info['id'] = zk_broker_id
            zk_broker_datetime = datetime.datetime.utcfromtimestamp(int(zk_broker_info['timestamp'])/1000.0)
            zk_broker_info['datetime'] = zk_broker_datetime.strftime('%Y-%m-%d %H:%M:%S')
            zk_broker_info['duration'] = str((datetime.datetime.utcnow() - zk_broker_datetime))
            zk_brokers_info[zk_broker_info['host']] = zk_broker_info

        zk.stop()
        return zk_brokers_info

    def get_controller_from_zookeeper(self):
        """Gets the Kafka controller information from Zookeeper as a dict.
        """
        zk_quorum = self.get_zookeeper_quorum()
        zk = self.zk_connect(zk_quorum)
        zk_controller_data, stat = zk.get(self.ZK_KAFKA_CONTROLLER_PATH)
        zk.stop()

        zk_controller = json.loads(zk_controller_data)
        zk_controller_datetime = datetime.datetime.utcfromtimestamp(int(zk_controller['timestamp'])/1000.0)
        zk_controller['datetime'] = zk_controller_datetime.strftime('%Y-%m-%d %H:%M:%S')
        zk_controller['duration'] = str((datetime.datetime.utcnow() - zk_controller_datetime))

        return zk_controller

    def get_stale_broker_hosts_from_ambari(self):
        """Gets Brokers from Ambari with Stale configs.
        """
        return self.get_stale_hosts_from_ambari(self.COMPONENT_KAFKA_BROKER)

    def get_stale_hosts_from_ambari(self, component_name):
        """Gets hosts from Ambari that have Stale configs for the given component_name
        """
        params = {'HostRoles/stale_configs' : 'true', 'HostRoles/component_name' : component_name}
        hosts_result = self.ambari_helper.request_url('clusters/{0}/host_components'.format(self.cluster_name), 'GET', params)

        stale_hosts = map(lambda m : m['HostRoles']['host_name'],
                        filter(lambda h : h['HostRoles']['host_name'].startswith(self.cluster_manifest.settings[Constants.WORKERNODE_VM_NAME_PREFIX_SETTING_KEY]), 
                            hosts_result['items']))
        self.logger.info('Hosts with stale configs for component {0}: {1}\n{2}\n'.format(component_name, len(stale_hosts), pprint.pformat(stale_hosts)))
        return stale_hosts

    def restart_kafka_broker_from_ambari(self, host_name):
        """Restart the Kafka Broker on the host.
        """
        return self.restart_component_from_ambari(host_name, self.SERVICE_KAFKA, self.COMPONENT_KAFKA_BROKER)
    
    def restart_component_from_ambari(self, host_name, service_name, component_name):
        """Restart the component for a service on the host.
        """
        self.stop_component_from_ambari(host_name, service_name, component_name)
        self.start_component_from_ambari(host_name, service_name, component_name)

    def get_component_from_ambari(self, host_name, service_name, component_name):
        """Gets the component state from Ambari for the host.
        """
        host_component_url = 'clusters/{0}/hosts/{1}/host_components/{2}'.format(self.cluster_name, host_name, component_name)
        return self.ambari_helper.query_url(host_component_url)

    def stop_component_from_ambari(self, host_name, service_name, component_name):
        """Stops the component on the host and waits until it stops.
        """
        self.change_host_component_state_from_ambari(host_name, service_name, component_name, self.STATE_INSTALLED)
        return self.wait_for_component_state_from_ambari(host_name, service_name, component_name, self.STATE_INSTALLED)

    def start_component_from_ambari(self, host_name, service_name, component_name):
        """Starts the component on the hosts and waits until it is started.
        """
        self.change_host_component_state_from_ambari(host_name, service_name, component_name, self.STATE_STARTED)
        return self.wait_for_component_state_from_ambari(host_name, service_name, component_name, self.STATE_STARTED)

    def wait_for_component_state_from_ambari(self, host_name, service_name, component_name, state):
        """Waits for the component to reach the desired state.
        """
        now = time.time()
        timeout = now + self.TIMEOUT_SECS
        while time.time() < timeout:
            component = self.get_component_from_ambari(host_name, service_name, component_name)
            if (component['HostRoles']['state'] == state):
                self.logger.debug('Component {0} on host {1} reached the desired state: {2}'.format(component_name, host_name, state))
                return component
            else:
                self.logger.debug('Component {0} on host {1} has not yet reached the desired state: {2}. Current state: {3}, Time Elapsed: {4:.2f}'.format(
                    component_name, host_name, state, component['HostRoles']['state'], (time.time() - now)))
            time.sleep(self.SLEEP_SECS)
        err_msg = 'Component {0} on host {1} did not reach the desired state: {2} in {3} secs.'.format(component_name, host_name, state, self.TIMEOUT_SECS)
        self.logger.error(err_msg)
        raise RuntimeError(err_msg)

    @retry(exceptions=BaseException, tries=Constants.MAX_RETRIES, delay=Constants.RETRY_INTERVAL_DELAY, backoff=Constants.RETRY_INTERVAL_BACKOFF)
    def change_host_component_state_from_ambari(self, host_name, service_name, component_name, state):
        """
        Convenience method for changing the state of a particular host component. State values can be 'STARTED' or 'INSTALLED'
        """
        host_component_url = 'clusters/{0}/hosts/{1}/host_components/{2}'.format(self.cluster_name, host_name, component_name)
        payload = {
            'RequestInfo': {
                'context' : 'Change host component state for host {0} and component {1} to state {2} via REST'.format(host_name, component_name, state)
            },
            'Body': {
                'HostRoles': {
                    'state': state
                }
            }
        }
        json_payload = json.dumps(payload)
        return self.ambari_helper.put_url(host_component_url, json_payload)

    def zk_connection_loss_check(self, state):
        """Checks for Zookeeper connection loss
        """
        if state == KazooState.LOST or state == KazooState.SUSPENDED:
            raise RuntimeError('Fatal error lost connection to zookeeper.')

    @retry(exceptions=BaseException, tries=3, delay=1, backoff=2)
    def zk_connect(self, zk_quorum):
        """Connect to the specified Zookeeper quorum using the connection string.
        """
        self.logger.debug('Connecting to zookeeper quorum at: {0}'.format(zk_quorum))
        zk = KazooClient(hosts=zk_quorum)
        zk.start()
        zk.add_listener(self.zk_connection_loss_check)
        return zk
