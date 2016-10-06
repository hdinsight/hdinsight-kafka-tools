import fnmatch, logging, os, sys, time
from kafka_testutils import KafkaTestUtils

logger = logging.getLogger(__name__)

def main(ktu):
    share_map = ktu.getShareNames()
    log_map = []
    index_map = []

    for share in share_map:
        for root, dirnames, filenames in os.walk(share):
            for filename in fnmatch.filter(filenames, '*.index'):
                index_map.append(os.path.join(root, filename))
            for filename in fnmatch.filter(filenames, '*.log'):
                log_map.append(os.path.join(root, filename))

    logger.info('Total number of index files: {0}'.format(len(index_map)))
    logger.info('Total number of log files: {0}'.format(len(log_map)))

    delete_map = []

    for index in index_map:
        index_statinfo = os.stat(index)
        index_log = index.replace('.index', '.log')
        if not index_log in log_map:
            logger.error('Missing log file for index: {0} with size: {1}'.format(index, index_statinfo.st_size))
            delete_map.append(index)
        else:
            index_log_statinfo = os.stat(index_log)
            logger.debug('Found log file: {0} with size: {1} for index: {2} with size: {3}'.format(index_log, index_log_statinfo.st_size, index, index_statinfo.st_size))
            if(index_log_statinfo.st_size == 0):
                delete_map.append(index)
                delete_map.append(index_log)

    for log in log_map:
        log_index = log.replace('.log','.index')
        log_statinfo = os.stat(log)
        if not log_index in index_map:
            logger.warn('Missing index file for log: {0} with size: {1}'.format(log, log_statinfo.st_size))
            delete_map.append(log)
        else:
            log_index_statinfo = os.stat(log_index)
            logger.debug('Found index file: {0} with size: {1} for log: {2} with size: {3}'.format(log_index, log_index_statinfo.st_size, log, log_statinfo.st_size))

    delete = False
    if len(sys.argv) > 1:
        delete = (sys.argv[1] == 'delete')
        logger.info('delete = ' + str(delete))

    logger.info('Total number of files to be deleted: {0}'.format(len(delete_map)))

    for delete in delete_map:
        logger.info('To be deleted: {0}'.format(delete))
        if delete:
            os.remove(delete)

if __name__ == '__main__':
    ktu = KafkaTestUtils(logger, 'kafkamissinglogindex{0}'.format(int(time.time())) + '.log')
    main(ktu)
