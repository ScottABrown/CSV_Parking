'''Process Creekside Village parking logs.
'''

from __future__ import print_function
import argparse

import json
import logging
import os
import uuid

import boto3

import csv_parking_log


FILENAME = os.path.split(__file__)[-1]
BASE_FILENAME = FILENAME[:-3] if FILENAME[-3:] == '.py' else FILENAME

# LOG_HANDLE = '%s_logger' % FILENAME
DEFAULT_LOG_FILE_NAME = '.'.join([BASE_FILENAME, 'log'])
DEFAULT_LOG_PATH = os.path.join(os.getcwd(), DEFAULT_LOG_FILE_NAME)

# The default name of the output file.
DEFAULT_JSON_OUTPUT_FILENAME = 'creekside_parking_data.json'

# The s3 buckets we use for file processing. The incoming bucket name
# we actually get from the event fired when a log is dropped in.
# DEFAULT_INCOMING_S3_BUCKET = 'creekside-parking-dropbox'
DEFAULT_OUTGOING_S3_BUCKET = 'creekside-parking'

DEFAULT_OUTGONG_ARCHIVE_PREFIX = 'archive'


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def argument_parser():
    '''Define command line arguments.'''

    parser = argparse.ArgumentParser(
        description='''
            Process Creekside Parking Logs and create JSON data file.
        '''
        )

    parser.add_argument(
        '-d', '--days',
        type=int,
        help='''
            number of days of records to read after start date or before
            end date, or before the last record date found in the Excel
            log file.
            '''
        )

    parser.add_argument(
        '-e', '--end-date',
        metavar='YYYY-MM-DD',
        help='''latest date for which to process parking records. '''
        )

    parser.add_argument(
        '-l', '--log-path',
        default=DEFAULT_LOG_PATH,
        help='''
            path to desired log file (DEFAULT: %s).
            ''' % DEFAULT_LOG_FILE_NAME
        )

    parser.add_argument(
        '--log',
        default=False,
        action='store_true',
        help='''write a log file (default: False).'''
        )

    parser.add_argument(
        '-o', '--output_file',
        # nargs='*',
        help='''
            JSON output file name.
            '''
        )

    parser.add_argument(
        '-s', '--start-date',
        metavar='YYYY-MM-DD',
        help='''earliest date for which to process parking records. '''
        )

    parser.add_argument(
        '-v', '--verbose',
        dest='verbose',
        default=0,
        action='count',
        help='''show more output.'''
        )

    parser.add_argument(
        'input_file',
        metavar="INPUT_FILE",
        # nargs='*',
        help='''
            Excel parking log file to process.
            '''
        )

    return parser


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def initialize_logging(args):
    '''Initialize loggers, handlers and formatters.

    A stream handler and file handler are added to the root logger.
    '''

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Logging handler for log file
    if args.log:
        fhandler = logging.FileHandler(args.log_path)
        fhandler.setLevel(logging.DEBUG)
        fformatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        fhandler.setFormatter(fformatter)
        logger.addHandler(fhandler)

    # Logging handler for stdout messages.
    shandler = logging.StreamHandler()

    sh_loglevel = [logging.WARNING, logging.INFO, logging.DEBUG]
    shandler.setLevel(sh_loglevel[min(args.verbose, 2)])

    # if shandler.level < logging.INFO:
    #     sformatter = logging.Formatter(
    #         '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    #         )
    #     shandler.setFormatter(sformatter)
    logger.addHandler(shandler)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def log_startup_configuration(args):
    '''Log execution start and configuration information.'''

    logger = logging.getLogger(__name__)

    logger.info('#%s', ' -' * 32)
    logger.info('#%sStarting %s', ' ' * 24, FILENAME)
    logger.info('#%s', ' -' * 32)
    logger.debug('Process PID: %s', os.getpid())
    logger.info('Log file is %s', args.log_path)

    logger.debug('Command line parameters:')
    for attr in [attr for attr in dir(args) if attr[0] != '_']:
        attr_log_entry = '    {:<16}\t{}'.format(attr, getattr(args, attr))
        logger.debug(attr_log_entry)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def process_workbook(args):
    '''Carry out workbook processing.'''

    log_parser = csv_parking_log.LogParser(args.input_file, days=args.days)
    log_parser.parse()
    dashboard_data = log_parser.dashboard_data()

    return dashboard_data


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def s3_event_handler(event, _):  # unused context parameter.
    '''Respond to an s3 event when called by AWS lambda.
    '''

    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    logger = logging.getLogger(__name__)
    logger.info('processing s3 event...')

    s3_client = boto3.client('s3')
    parser = argument_parser()

    logger.info('record count: %s', len(event['Records']))
    for record in event['Records']:

        inbucket = record['s3']['bucket']['name']
        # Bucket should be creekside-parking-dropbox.
        outbucket = DEFAULT_OUTGOING_S3_BUCKET

        key = record['s3']['object']['key']

        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        dashboard_data_upload_path = os.path.join(
            os.sep, 'tmp', DEFAULT_JSON_OUTPUT_FILENAME
            )

        logger.info('inbucket is %s', inbucket)
        logger.info('outbucket is %s', outbucket)
        logger.info('key is %s', key)
        logger.info('download_path is %s', download_path)
        logger.info(
            'dashboard_data_upload_path is %s', dashboard_data_upload_path
            )

        args = parser.parse_args(
            [
                '-d', '91',
                '-o', dashboard_data_upload_path,
                download_path
                ]
            )

        s3_client.download_file(inbucket, key, download_path)
        dashboard_data = process_workbook(args)
        logger.info('date range processed: %s', dashboard_data['date_range'])

        logger.info(
            'writing dashboard data to %s...', dashboard_data_upload_path
            )
        with open(dashboard_data_upload_path, 'w') as fptr:
            json.dump(dashboard_data, fptr)

        last_record_date = dashboard_data['date_range']['last_record_date']

        # Define path locations.
        output_object_key = DEFAULT_JSON_OUTPUT_FILENAME
        output_archive_object_key = '/'.join([
            DEFAULT_OUTGONG_ARCHIVE_PREFIX,
            '.'.join([output_object_key, last_record_date])
            ])
        xlsx_archive_object_key = '/'.join([
            DEFAULT_OUTGONG_ARCHIVE_PREFIX,
            '.'.join(['CreeksideParkingLog', last_record_date, 'xlsx'])
            ])
        logger.info(
            'output_archive_object_key is %s',
            output_archive_object_key
            )
        logger.info(
            'xlsx_archive_object_key is %s',
            xlsx_archive_object_key
            )

        # Create the active JSON data file.
        logger.info(
            'uploading %s to %s/%s...',
            dashboard_data_upload_path, outbucket, output_object_key
            )
        s3_client.upload_file(
            dashboard_data_upload_path, outbucket, output_object_key
            )
        logger.info(
            'making %s/%s publicly readable...',
            outbucket, output_object_key
            )
        s3_client.put_object_acl(
            ACL='public-read',
            Bucket=outbucket,
            Key=output_object_key
            )

        # Create a datestamped copy of this JSON data.
        logger.info(
            'uploading datestamped %s to %s/%s...',
            dashboard_data_upload_path, outbucket, output_archive_object_key
            )
        s3_client.upload_file(
            dashboard_data_upload_path, outbucket, output_archive_object_key
            )
        logger.info(
            'making %s/%s publicly readable...',
            outbucket, output_archive_object_key
            )
        s3_client.put_object_acl(
            ACL='public-read',
            Bucket=outbucket,
            Key=output_archive_object_key
            )

        # Create a copy of the incoming log spreadsheet.
        logger.info(
            'making copy of %s at %s/%s...',
            dashboard_data_upload_path, outbucket, xlsx_archive_object_key
            )
        s3_client.upload_file(
            dashboard_data_upload_path, outbucket, xlsx_archive_object_key
            )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def main():
    '''Main program entry point.'''

    logger = logging.getLogger(__name__)

    parser = argument_parser()
    args = parser.parse_args()

    initialize_logging(args)
    log_startup_configuration(args)

    dashboard_data = process_workbook(args)

    if args.output_file:
        logger.info('writing dashboard data to %s...', args.output_file)
        with open(args.output_file, 'w') as fptr:
            json.dump(dashboard_data, fptr)
    else:
        logger.info('no dashboard data output file specified.')

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
    main()
