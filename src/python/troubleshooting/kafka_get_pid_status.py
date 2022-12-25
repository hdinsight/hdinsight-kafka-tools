"""Script to report the status of the Kafka process on this current host.
"""
import datetime
import os
import psutil

kafka_pid_file = '/var/run/kafka/kafka.pid'

if os.path.exists(kafka_pid_file):
    with open(kafka_pid_file, "r") as kpf:
        kafka_pid = kpf.readline().strip()
    int_kafka_pid = int(kafka_pid)
    if int_kafka_pid > 0 and psutil.pid_exists(int_kafka_pid):
        kafka_process = psutil.Process(int_kafka_pid)
        kafka_process_status = kafka_process.status()

        # Reboot if Kafka process has been zombie for more than max_kafka_zombie_secs seconds
        if kafka_process_status == psutil.STATUS_ZOMBIE:
            print('ERROR: Kafka process with pid [{0}] is in a zombie state: {1}'.
                  format(kafka_pid, kafka_process_status))
        else:
            print('SUCCESS: Verification of Kafka successful! Kafka pid: {0}, status: {1}, create_time: {2}'.
                  format(kafka_pid, kafka_process_status,
                         datetime.datetime.fromtimestamp(kafka_process.create_time()).strftime("%Y-%m-%d %H:%M:%S")))
    else:
        print('ERROR: Kafka is not running. Invalid pid file: {0}, pid: {1}'.format(kafka_pid_file, int_kafka_pid))
else:
    print('ERROR: Kafka pid file: {0} not found, Kafka is not running'.format(kafka_pid_file))
