
'''
test_csv_parking_log_parser.py

Test cases for the csv_parking_record.LogParser class.
'''

import unittest
from zipfile import BadZipfile

from xlrd import XLRDError

import csv_parking_log
from csv_parking_log import CsvParkingLogStructureError

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Uncomment to show lower level logging statements.
import logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
shandler = logging.StreamHandler()
shandler.setLevel(logging.INFO)  # Pick one.
shandler.setLevel(logging.DEBUG)  # Pick one.
logger.addHandler(shandler)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class TestLogParser(unittest.TestCase):
    '''
    Basic test cases for LogParser.
    '''

    # pylint: disable=invalid-name

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def setUp(self):
        '''Test case common fixture setup.
        '''
        pass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_init_errors(self):
        '''Test LogParser initialization errors.
        '''

        with self.assertRaises(ValueError):
            _ = csv_parking_log.LogParser(
                filepath='empty.txt',
                days=30,
                start_date="2016-01-01",
                end_date="2016-01-31"
                )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_init(self):
        '''Test initialization of a LogParser instance.
        '''

        log_parser = csv_parking_log.LogParser(filepath='empty.txt')
        self.assertIsNotNone(log_parser)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse(self):
        '''Test LogParser.parse.
        '''

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_empty.txt'
            )

        with self.assertRaises(XLRDError):
            log_parser.parse()

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_corrupt.xlsx'
            )

        with self.assertRaises(BadZipfile):
            log_parser.parse()

        # TODO: Add "no data sheet".
        # # - - - - - - - - - - - - - - - -
        # log_parser = csv_parking_log.LogParser(
        #     filepath='sample_log_no_data_sheet.xlsx'
        #     )

        # with self.assertRaises(CsvParkingLogStructureError):
        #     log_parser.parse()

        # TODO: Add "data sheet in Sheet 2".
        # # - - - - - - - - - - - - - - - -
        # log_parser = csv_parking_log.LogParser(
        #     filepath='sample_log_data_sheet_2.xlsx'
        #     )

        # with self.assertRaises(CsvParkingLogStructureError):
        #     log_parser.parse()

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_headers_only.xlsx'
            )

        log_parser.parse()

        # logger.info('setting log level to DEBUG')
        # log_level = logging.DEBUG
        # logger.setLevel(log_level)

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_one_record.xlsx'
            )

        log_parser.parse()

        # logger.info('setting log level to INFO')
        # log_level = logging.DEBUG
        # logger.setLevel(log_level)

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_typemix.xlsx'
            )

        log_parser.parse()

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_30_lines.xlsx'
            )

        log_parser.parse()

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_30_lines.xlsx',
            days=6
            )

        log_parser.parse()

        self.assertTrue(True)
        self.assertEqual(1, 1)


        # TODO: Test cases with days, start_date, end_date combinations.

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define test suite.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# pylint: disable=invalid-name
load_case = unittest.TestLoader().loadTestsFromTestCase
all_suites = {
    # Lowercase these.
    'suite_LogParser': load_case(
        TestLogParser
        ),
    }

master_suite = unittest.TestSuite(all_suites.values())
# pylint: enable=invalid-name

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == '__main__':
    unittest.main()
