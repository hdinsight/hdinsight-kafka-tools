"""
OVERVIEW:
=========
Get Kafka metrics from Ambari.

RUNNING THE SCRIPT:
===================
You need to run this script with sudo privilege due to permission issues on some python packages:
sudo python metrics.py -h

Tested on HDInsight 3.2 (Linux Ubuntu 12) with HDP 2.2.7.1 and Kafka 0.8.1.1

REFERENCES:
===========
https://cwiki.apache.org/confluence/display/AMBARI/Stack+Defined+Metrics
https://cwiki.apache.org/confluence/display/AMBARI/Ambari+Metrics+API+specification
"""

import logging
import json
import os.path
import calendar
import time
import argparse

from hdinsight_common import hdinsightlogging
from hdinsight_common.AmbariHelper import AmbariHelper

logger = logging.getLogger(__name__)
KAFKA_METRICS_DESCRIPTOR_URL = 'stacks/HDP/versions/2.2/services/KAFKA/artifacts/metrics_descriptor'
KAFKA_METRICS_BASE_URL = 'clusters/{0}/services/KAFKA/components/KAFKA_BROKER?fields='

AMBARI_METRICS = 'AMBARI_METRICS'

def execute(temporal):
    logger.info('Getting Kafka metrics descriptor from %s', KAFKA_METRICS_DESCRIPTOR_URL)
    a = AmbariHelper()
    kafka_metrics_descriptor = a.query_url(KAFKA_METRICS_DESCRIPTOR_URL)
    clusters_result = a.query_url('clusters')
    
    kafka_metrics = {}
    kafka_metrics_url = KAFKA_METRICS_BASE_URL.format(clusters_result['items'][0]['Clusters']['cluster_name'])
    idx = 0
    
    end_epoch = calendar.timegm(time.gmtime())
    start_epoch = end_epoch - temporal
    
    for i in kafka_metrics_descriptor['artifact_data']['KAFKA']['KAFKA_BROKER']['Component'][0]['metrics']['default']:
        if ((temporal > 0) and kafka_metrics_descriptor['artifact_data']['KAFKA']['KAFKA_BROKER']['Component'][0]['metrics']['default'][i]['temporal']):
            metric = i + '[{0},{1},15]'.format(start_epoch, end_epoch)
        else:
            metric = i
        if metric:
            if idx > 0:
                kafka_metrics_url += ','
            kafka_metrics_url += metric
            idx += 1

    if temporal > 0:
        kafka_metrics_url += '&_{0}'.format(1000*end_epoch)

    logger.info('Querying Metrics Url: %s', kafka_metrics_url)
    kafka_metrics = a.query_url(kafka_metrics_url)
    logger.info('Kafka Metrics:\r\n%s', kafka_metrics)

    if 'metrics' in kafka_metrics:
        return kafka_metrics
    else:
        error_msg = 'Kafka metrics unavailable. Please check the status of Ambari Metrics Server or trying doing a temporal query using --temporal parameter.'
        logger.error(error_msg)
        ambari_metrics_service = a.get_service_info(AMBARI_METRICS)
        logger.debug('Ambari metrics service:\r\n%s', ambari_metrics_service)
        raise RuntimeError(error_msg)

def main():
    parser = argparse.ArgumentParser(description='Get Kafka metrics from Ambari')
    parser.add_argument('-t', '--temporal', nargs='?', type=int, const='300', default='0', 
        help='specify the time interval (in seconds) to get temporal metrics (default: last 5 minutes metrics are returned). NOT specifying this argument will return current point-in-time metrics')

    args = parser.parse_args()

    logger.info('Ambari metrics mode - Temporal: %s', args.temporal)
    kafka_metrics = execute(args.temporal)

if __name__ == "__main__":
    hdinsightlogging.initialize_root_logger()
    main()
