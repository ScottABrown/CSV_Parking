'''Process Creekside Village parking logs.
'''

from __future__ import print_function
import argparse
from datetime import datetime
from datetime import timedelta

import json
import logging
import os
import re
import sys
import uuid


import boto3
import xlrd

import matchiness
import csv_parking_log


FILENAME = os.path.split(__file__)[-1]
BASE_FILENAME = FILENAME[:-3] if FILENAME[-3:] == '.py' else FILENAME

# LOG_HANDLE = '%s_logger' % FILENAME
DEFAULT_LOG_FILE_NAME = '.'.join([BASE_FILENAME, 'log'])
DEFAULT_LOG_PATH = os.path.join(os.getcwd(), DEFAULT_LOG_FILE_NAME)


# THe text expected in row 0 of the Code3 parking spreadsheet.
EXPECTED_ROW_0 = [
    'MAKE',
    'MODEL',
    'COLOR',
    'LIC',
    'LOCATION',
    '1ST',
    '2nd',
    '3rd',
    'TOWED',
    '1ST',
    'TOWED',
    ]

# Indices for rows of the Code3 parking spreadsheet.
COL_INDICES = {
    'MAKE': 0,
    'MODEL': 1,
    'COLOR': 2,
    'LIC': 3,
    'LOCATION': 4,
    'OPEN_PARKING_1': 5,
    'OPEN_PARKING_2': 6,
    'OPEN_PARKING_3': 7,
    'TOWDATE': 8,
    'STREET_PARKING_1': 9,
    'TOWDATE_2': 10,
    }

# The log record type, based on the column in which the date is logged.
RECORD_TYPE = {
    5: 'guest_1',
    6: 'guest_2',
    7: 'guest_3',
    8: 'guest_tow',
    9: 'street_1',
    10: 'street_tow',
    }

# Date comparison is easier when we use "days since ref date" to compare.
REF_DATETIME = datetime(2000, 01, 01)

# This is the Excel vslue for offset to January 1, 2000.
EXCEL_OFFSET_TO_REF_DATETIME = 36526

# The name of the key for the days-since-REF_DATETIME, so we encode the
# REF_DATETIME in the key.
REF_DATETIME_KEY = 'days_since_{}'.format(REF_DATETIME.strftime('%Y%m%d'))

# The length of the windows over which to summarize the number of logs
# for each license.
WINDOW_DAYS = 30

# The default name of the output file.
DEFAULT_JSON_OUTPUT_FILENAME = 'canonical_lic.json'

# The s3 buckets we use for file processing.
DEFAULT_INCOMING_S3_BUCKET = 'creekside-parking-dropbox'
DEFAULT_OUTGOING_S3_BUCKET = 'creekside-parking'

DEFAULT_OUTGONG_ARCHIVE_PREFIX = 'archive'

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def argument_parser():
    '''
    Define command line arguments.
    '''

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
    '''Log execution start and configuration information.
    '''

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
def days_since_refdate(end_dt, refdate=REF_DATETIME):
    '''Return the number of days from refdate to end_dt.
    '''

    return (end_dt - refdate).days


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def log_date_to_datetime(log_date):
    '''Convert a parking log date to a datetime object.
    '''
    # log_date_pattern = r'(?P<log_date>\d\d\.\d\d\.\d\d)'
    log_date_pattern = (
        r'(?P<log_mon>\d{1,2})\.(?P<log_day>\d{1,2})\.(?P<log_year>\d{1,2})'
        )

    # The old date format was m.dd.yy dot separated. The new format is an
    # Excel date value.

    if isinstance(log_date, float):
        # It's Excel's offset from "January 0, 1900". Also, Excel incorrectly
        # treats 1900 as a leap year. Argh.

        log_date_as_datetime = (
            REF_DATETIME + timedelta(log_date - EXCEL_OFFSET_TO_REF_DATETIME)
            )
        # print('{}:\t{}'.format(log_date, log_date_as_datetime))
        return log_date_as_datetime

    else:
        matches = re.match(log_date_pattern, log_date)
        if matches:
            log_date = '{:0>2}.{:0>2}.{:0>2}'.format(
                matches.group('log_mon'),
                matches.group('log_day'),
                matches.group('log_year')
                )
        else:
            raise ValueError('No log date found in %s' % log_date)

        return datetime.strptime(log_date, '%m.%d.%y')


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def require_expected_row_0(sheet):
    '''Raise an exception if row 0 of a sheet isn't as required.
    '''
    # for col_num in [
    #         MAKE, MODEL, COLOR, LIC, LOCATION,
    #         OPEN_PARKING_1, OPEN_PARKING_2, OPEN_PARKING_3,
    #         TOWDATE, STREET_PARKING_1, TOWDATE_2,
    #         ]:  # pylint: disable=bad-continuation
    for col_num in COL_INDICES.values():  # pylint: disable=bad-continuation
        # print sheet.row(0)[col_num].value
        expected_cell_value = EXPECTED_ROW_0[col_num]
        actual_cell_value = sheet.row(0)[col_num].value
        if  expected_cell_value.upper() not in actual_cell_value.upper():
            err_msg = '%s: row 0, column %s: expected "%s", found "%s"'
            raise RuntimeError(
                err_msg % (
                    sheet.name, col_num,
                    expected_cell_value, actual_cell_value
                    )
                )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def get_latest_valid_refdt_offset(filename):
    '''Try to find the latest refdt offset that should be considered valid.
    '''
    logger = logging.getLogger(__name__)

    filename_pieces = filename.split('.')
    refdt_values = []

    for piece in filename_pieces:
        try:
            dt_value = datetime.strptime(piece, '%Y%m%d')
            refdt_values.append(days_since_refdate(dt_value))
        except ValueError:
            pass

    # We really expect at most one refdt value in the filename.
    if len(refdt_values) > 1:
        logger.warn(
            'multiple refdt values found in filename: %s', refdt_values
            )

    if len(refdt_values) == 0:
        return days_since_refdate(datetime.now())
    else:
        return max(refdt_values)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def inprocess_xls(args):”


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def days_between_datesets(dateset1, dateset2, to_datetime_method):
    '''Return the number of days between closest dates in a set of dates.
    '''

    # days_1 = [(to_datetime_method(x) - REF_DATETIME).days for x in dateset1]
    # days_2 = [(to_datetime_method(x) - REF_DATETIME).days for x in dateset2]

    days_1 = [days_since_refdate(to_datetime_method(x)) for x in dateset1]
    days_2 = [days_since_refdate(to_datetime_method(x)) for x in dateset2]

    days_between = 0

    if max(days_1) < min(days_2):
        days_between = min(days_2) - max(days_1)

    elif max(days_2) < min(days_1):
        days_between = min(days_1) - max(days_2)

    return days_between


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def refine_plate_equivalents(plate_list, record_index):
    '''Separate plate list members that were incorrectly grouped.

    Arguments:

        plate_list (list of str):
            A list of plates that were grouped as equivalent by
            ``matchiness.find_equivalence_classes()``.

        record_index (dict):
            A dictionary with a sub-dictionary record_index['LIC']
            whose keys are plates and whose values are the records
            extracted from a parking log for that plate.
    '''

    if len(plate_list) == 1:
        return [plate_list]

    # - - - - - - - - - - - - - - - - - - - - - - - -
    #
    #
    # TODO: remove this:
    #
    #
    # - - - - - - - - - - - - - - - - - - - - - - - -
    return [plate_list]
    # - - - - - - - - - - - - - - - - - - - - - - - -
    #
    #
    #
    #
    #
    # - - - - - - - - - - - - - - - - - - - - - - - -

    plate_pairs_fields = {}
    plate_pairs_comparison = {}
    plate_list = sorted(plate_list)

    for plate_1 in plate_list:

        records_1 = record_index['LIC'][plate_1]

        for plate_2 in plate_list[plate_list.index(plate_1) + 1:]:
            records_2 = record_index['LIC'][plate_2]

            plate_pair = tuple([plate_1, plate_2])

            plate_pairs_fields[plate_pair] = {
                'makes': [
                    set([x['raw_make'] for x in records_1]),
                    set([x['raw_make'] for x in records_2])
                    ],
                'models': [
                    set([x['raw_model'] for x in records_1]),
                    set([x['raw_model'] for x in records_2])
                    ],
                'colors': [
                    set([x['raw_color'] for x in records_1]),
                    set([x['raw_color'] for x in records_2])
                    ],
                'dates': [
                    set([x['raw_date'] for x in records_1]),
                    set([x['raw_date'] for x in records_2])
                    ],
                }
            fields = plate_pairs_fields[plate_pair]
            plate_pairs_comparison[plate_pair] = {
                k: {
                    'unique_1': fields[k][0].difference(fields[k][1]),
                    'common': fields[k][0].intersection(fields[k][1]),
                    'unique_2': fields[k][1].difference(fields[k][0]),
                    'all': fields[k][0].union(fields[k][1]),
                    }
                for k in fields.keys()
                }

            # if(
            #         plate_pairs_comparison[plate_pair]['models']['common']
            #         != plate_pairs_comparison[plate_pair]['models']['all']
            #         ):  # pylint: disable=bad-continuation

            #     print '\n----\n'
            #     print (
            #         plate_pairs_comparison[plate_pair]['models']['common']
            #         != plate_pairs_comparison[plate_pair]['models']['all']
            #         )
            #     print plate_list
            #     print '{}: {}'.format(plate_1, len(records_1))
            #     print '{}: {}'.format(plate_2, len(records_2))

            #     print "Records:"
            #     for x in records_1:
            #         print 'record 1: {}: {}'.format(plate_1, x)
            #     for x in records_2:
            #         print 'record 2: {}: {}'.format(plate_2, x)

            #     print "Plate pairs comparison:"
            #     for k, v in plate_pairs_comparison[plate_pair].items():
            #         print '{}:\t{}'.format(k, v)
            #     print
            #     match_score = matchiness.get_match_score(list(plate_pair))
            #     print (
            #         '{}\t({}, {})\tCommon scores: ({}, {}, {})\t{}\t({}, {})'
            #         ).format(
            #         plate_pair,
            #         len(record_index['LIC'][plate_pair[0]]),
            #         len(record_index['LIC'][plate_pair[1]]),
            #         len(plate_pairs_comparison[plate_pair]['makes']['common']),
            #         len(
            #             plate_pairs_comparison[plate_pair]['models']['common']
            #             ),
            #         len(
            #             plate_pairs_comparison[plate_pair]['colors']['common']
            #             ),
            #         days_between_datesets(
            #             [x['raw_date'] for x in records_1],
            #             [x['raw_date'] for x in records_2],
            #             log_date_to_datetime
            #             ),
            #         match_score[plate_pair[0]][plate_pair[1]],
            #         match_score[plate_pair[1]][plate_pair[0]],
            #         )

            #     import sys
            #     sys.exit()
    # Count number of occurrences of make, model, color; lower value if common
    # matches. knocking out edges from complete graph.


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def select_canonical_lic(plate_set):
    '''Select and return the canonical representative of a set of plates.
    '''
    return list(plate_set)[0]


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def populate_canonical_licenses(record_index, plates):
    '''Refine the plate equivalents and select canonical representatives.
    '''
    matches = matchiness.find_equivalence_classes(list(plates))

    for plate_list in matches:

        # old_thing_with_match_groups(plate_list)
        refined_plate_list = refine_plate_equivalents(plate_list, record_index)

        for plate_list_section in refined_plate_list:

            # canonical_lic = list(plate_list_section)[0]
            canonical_lic = select_canonical_lic(plate_list_section)
            record_index['CANONICAL_LIC'][canonical_lic] = []

            for plate in list(plate_list_section):

                for record in record_index['LIC'][plate]:
                    record['canonical_lic'] = canonical_lic
                    record['lic_equivalents'] = list(plate_list_section)

                record_index['CANONICAL_LIC'][canonical_lic].extend(
                    record_index['LIC'][plate]
                    )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def old_thing_with_match_groups(plate_list):
    '''obsolete.
    '''
    _ = plate_list  # Shut up, pylint.
        # if len(plate_list) > 1:
        #     multi_match_count += 1

        #     # Dump match groups.
        #     print '\n\n\n-----------------------------\n\n'
        #     print 'group {}\t{}'.format(
        #         multi_match_count, "\t".join(plate_list)
        #         )

        #     for plate in plate_list:

        #         # if plate not in record_index_by_lic:
        #         if plate not in record_index['LIC']:
        #             # TODO: log a warning.
        #             print '{}: No record'.format(plate)
        #             continue

        #         for record in record_index_by_lic[plate]:
        #             print '{}\t{}\t{}\t{}\t{}'.format(
        #                 record['raw_lic'],
        #                 record['raw_make'],
        #                 record['raw_model'],
        #                 record['raw_color'],
        #                 record['raw_date'],
        #                 )
        #     print


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def remove_date_duplicate_records(lic_records):
    '''Mark all but one record date duplicate for a set of license records.

    Arguments:
        lic_records (list):
            A list of all the records that share a common value of
            ``canonical_lic``.canonical_lic

    '''

    lic_records.sort(key=lambda x: x['days_since_20000101'])

    records_index_by_refdate_offset = {}
    record_stats = {
        'raw_make': {},
        'raw_model': {},
        'raw_color': {}
        }
    stat_weight = {
        'raw_make': 2,
        'raw_model': 3,
        'raw_color': 1
        }

    for record in lic_records:
        _ = records_index_by_refdate_offset.setdefault(record['days_since_20000101'], [])
        records_index_by_refdate_offset[record['days_since_20000101']].append(record)
        for stat in record_stats.keys():
            _ = record_stats[stat].setdefault(record[stat], 0)
            record_stats[stat][record[stat]] += 1

    # These are the indices (refdates) in records_index_by_refdate_offset where there
    # are multiple records for the license on that refdate.
    duplicate_records_sets = [
        records_index_by_refdate_offset[d]
        for d in records_index_by_refdate_offset
        if len(records_index_by_refdate_offset[d]) > 1
        ]

    for duplicate_records in duplicate_records_sets:
        for record in duplicate_records:

            record['duplicate'] = True
            record['scores'] = {
                stat: (
                    stat_weight[stat]
                    * float(record_stats[stat][record[stat]])
                    / len(lic_records)
                    )
                for stat in record_stats.keys()
                }
            record['scores']['total'] = (
                record['scores']['raw_make']
                + record['scores']['raw_model']
                + record['scores']['raw_color']
                )

        max_total_score = (
            max([r['scores']['total'] for r in duplicate_records])
            )

        for record in duplicate_records:
            if record['scores']['total'] == max_total_score:
                record['duplicate'] = False
                break

        for record in duplicate_records:
            del record['scores']

        # print json.dumps(lic_records)
        # print json.dumps(duplicate_records)
        # exit(0)

    lic_records = [
        r for r in lic_records if 'duplicate' not in r or not r['duplicate']
        ]
    # print json.dumps([r for r in lic_records if 'duplicate' in r])

    return lic_records

    # for index, record in enumerate(lic_records):
    #     if index > 0 and (
    #             lic_records[index - 1]['days_since_20000101']
    #             == record['days_since_20000101']
    #             ):  # pylint: disable=bad-continuation
    #         _ = duplicate_records.setdefault(
    #             record['days_since_20000101'], []
    #             )
    #         duplicate_records[record['days_since_20000101']].append(
    #             (index - 1, lic_records[index - 1])
    #             )
    #         duplicate_records[record['days_since_20000101']].append(
    #             (index, record)
    #             )
    #         # # Inefficient to do this each time, but it's infrequent
    #         # # and this is easy.
    #         # duplicate_records[record['days_since_20000101']] = list(set(
    #         #     duplicate_records[record['days_since_20000101']]
    #         #     ))

    # if duplicate_records:
    #     print json.dumps(duplicate_records)
    #     exit(0)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def postprocess_output_data(output_data):
    '''Add statistics and other final processing for output data.

    - Move

    '''
    records_by_lic = output_data['records_by_lic']
    last_offset = output_data['date_range']['last_record_refdt_offset']
    logged_windows = [(n + 1) * WINDOW_DAYS for n in range(3)]

    # Remove duplicates, and move records under a 'records' key.
    for (lic, records) in records_by_lic.items():

        # This will also sort the records.
        records_by_lic[lic] = {
            'canonical_lic': records[0]['canonical_lic'],
            'lic_equivalents': records[0]['lic_equivalents'],
            'records': remove_date_duplicate_records(records)
            }

    # Calculate five day totals.
    for lic in records_by_lic:
        records = records_by_lic[lic]['records']
        index_a = 0
        for index_b, record in enumerate(records):
            while (
                    records[index_b]['days_since_20000101'] - 5
                        >= records[index_a]['days_since_20000101']
                    ):  # pylint: disable=bad-continuation
                index_a += 1
            # We've now moved index_a up until the days between the
            # records at index_a and index_b is no more than 5.
            record['five_day_total'] = index_b - index_a + 1

    # Calculate 30/60/90 day totals.
    for lic in records_by_lic:
        record_now = records_by_lic[lic]
        window_total_now = {
            'logged': {x: 0 for x in logged_windows},
            'last_five': {x: 0 for x in logged_windows},
            }
        # window_total_now = record_now['window_total']
        records = record_now['records']
        for record in records:
            window = (
                1 + int(
                    (last_offset - record['days_since_20000101']) / WINDOW_DAYS
                    )
                ) * WINDOW_DAYS

            # Skip any that we aren't totaling for.
            if window not in logged_windows:
                continue

            for record_window in logged_windows:
                if window > record_window:
                    continue
                window_total_now['logged'][record_window] += 1
                if record['five_day_total'] >= 3:
                    window_total_now['last_five'][record_window] += 1

        # This is a bit of a hack to facilitate the record extraction in the
        # dashboard.
        record_now['window_total'] = [
          {
            'key': 'log1-long',
            'value': window_total_now['logged'][logged_windows[2]]
            },
          {
            'key': 'log1-medium',
            'value': window_total_now['logged'][logged_windows[1]]
            },
          {
            'key': 'log1-short',
            'value': window_total_now['logged'][logged_windows[0]]
            },
          {
            'key': 'log5-long',
            'value': window_total_now['last_five'][logged_windows[2]]
            },
          {
            'key': 'log5-medium',
            'value': window_total_now['last_five'][logged_windows[1]]
            },
          {
            'key': 'log5-short',
            'value': window_total_now['last_five'][logged_windows[0]]
            },
            ]

        # window_total_now['log1-long'] = window_total_now['logged'][logged_windows[2]]
        # window_total_now['log1-medium'] = window_total_now['logged'][logged_windows[1]]
        # window_total_now['log1-short'] = window_total_now['logged'][logged_windows[0]]
        # window_total_now['log5-long'] = window_total_now['last_five'][logged_windows[2]]
        # window_total_now['log5-medium'] = window_total_now['last_five'][logged_windows[1]]
        # window_total_now['log5-short'] = window_total_now['last_five'][logged_windows[0]]

    # exit()

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def process_workbook(args):
    '''Carry out workbook processing. 
    '''
    log_records = inprocess_xls(args)
    # date_range, plates, record_index = inprocess_xls(args)

    assert(False)

    # record_index['CANONICAL_LIC'] = {}

    # populate_canonical_licenses(record_index, plates)

    # output_data = {
    #     'date_range': date_range,
    #     'records_by_lic': record_index['CANONICAL_LIC'],
    #     }

    # postprocess_output_data(output_data)

    # if args.output_file:
    #     with open(args.output_file, 'w') as fptr:
    #         json.dump(output_data, fptr)
    # else:
    #     print(json.dumps(output_data))

    # # In case this is useful downstream.
    # return output_data
     
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def s3_event_handler(event, _):  # unused context parameter.
    '''Respond to an s3 event when called by AWS lambda.
    '''

    s3_client = boto3.client('s3')
    parser = argument_parser()

    for record in event['Records']:

        inbucket = record['s3']['bucket']['name']
        # Bucket should be creekside-parking-dropbox.
        outbucket = DEFAULT_OUTGOING_S3_BUCKET

        key = record['s3']['object']['key']

        download_path = '/tmp/{}{}'.format(uuid.uuid4(), key)
        upload_path = os.path.join(os.sep, 'tmp', DEFAULT_JSON_OUTPUT_FILENAME)
        
        args = parser.parse_args(['-d', '91', '-o', upload_path, download_path])

        s3_client.download_file(inbucket, key, download_path)
        output_data = process_workbook(args)
        last_record_date = output_data['date_range']['last_record_date']

        output_object_key = DEFAULT_JSON_OUTPUT_FILENAME
        output_archive_object_key = '/'.join([
            DEFAULT_OUTGONG_ARCHIVE_PREFIX,
            '.'.join([output_object_key, last_record_date])
            ])
        xlsx_archive_object_key = '/'.join([
            DEFAULT_OUTGONG_ARCHIVE_PREFIX,
            '.'.join(['CreeksideParkingLog', last_record_date, 'xlsx'])
            ])

        # Create the active JSON data file.
        s3_client.upload_file(
            upload_path, outbucket, output_object_key
            )
        s3_client.put_object_acl(
            ACL='public-read',
            Bucket=outbucket,
            Key=output_object_key
            )

        # Create a datestamped copy of this JSON data.
        s3_client.upload_file(
            upload_path, outbucket, output_archive_object_key)
        s3_client.put_object_acl(
            ACL='public-read',
            Bucket=outbucket,
            Key=output_archive_object_key
            )

        # Create a copy of the incoming log spreadsheet.
        s3_client.upload_file(
            upload_path, outbucket, xlsx_archive_object_key
            )

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def main():
    '''Main program entry point.
    '''

    parser = argument_parser()
    args = parser.parse_args()

    initialize_logging(args)
    log_startup_configuration(args)

    process_workbook(args)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
    main()
