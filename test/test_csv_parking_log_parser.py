
'''
test_csv_parking_log_parser.py

Test cases for the csv_parking_record.LogParser class.
'''

import unittest
from zipfile import BadZipfile

from xlrd import XLRDError

import csv_parking_log
# from csv_parking_log import CsvParkingLogStructureError

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Uncomment to show lower level logging statements.
# import logging
# logger = logging.getLogger()
# logger.setLevel(logging.DEBUG)
# shandler = logging.StreamHandler()
# shandler.setLevel(logging.INFO)  # Pick one.
# shandler.setLevel(logging.DEBUG)  # Pick one.
# logger.addHandler(shandler)


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
    def test_csv_parking_log_parser_parse_empty(self):
        '''Test LogParser.parse with an empty log file.
        '''

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_empty.txt'
            )

        with self.assertRaises(XLRDError):
            log_parser.parse()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse_corrupt(self):
        '''Test LogParser.parse with a corrupt log file.
        '''

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_corrupt.xlsx'
            )

        with self.assertRaises(BadZipfile):
            log_parser.parse()

    # # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # def test_csv_parking_log_parser_parse_no_data_sheet(self):
    #     '''Test LogParser.parse with a no data sheet log file.
    #     '''
    #     # TODO: Add "no data sheet".
    #     # # - - - - - - - - - - - - - - - -
    #     # log_parser = csv_parking_log.LogParser(
    #     #     filepath='sample_log_no_data_sheet.xlsx'
    #     #     )

    #     # with self.assertRaises(CsvParkingLogStructureError):
    #     #     log_parser.parse()

    # # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # def test_csv_parking_log_parser_parse_sheet_2_data(self):
    #     '''Test LogParser.parse with a no data in sheet 2 log file.
    #     '''
    #     # TODO: Add "data sheet in Sheet 2".
    #     # # - - - - - - - - - - - - - - - -
    #     # log_parser = csv_parking_log.LogParser(
    #     #     filepath='sample_log_data_sheet_2.xlsx'
    #     #     )

    #     # with self.assertRaises(CsvParkingLogStructureError):
    #     #     log_parser.parse()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse_headers_only(self):
        '''Test LogParser.parse with a headers only log file.'''

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_headers_only.xlsx'
            )
        # pylint: disable=protected-access

        log_parser.parse()

        self.assertEqual(log_parser.column_manager.log_version, 'CSVPL16.1')
        self.assertEqual(log_parser.start_date, None)
        self.assertEqual(log_parser.end_date, None)
        self.assertEqual(log_parser.days, None)
        self.assertEqual(log_parser.start_refdt_offset, 0)
        self.assertEqual(
            log_parser.end_refdt_offset,
            csv_parking_log.DEFAULT_END_REFDT_OFFSET
            )
        self.assertEqual(log_parser.log_records, [])
        self.assertEqual(log_parser._plate_index, {})
        self.assertEqual(log_parser._canonical_plate_index, {})
        self.assertEqual(log_parser._plate_record_set_index, {})
        self.assertTrue(log_parser.max_valid_refdt_offset > 0)
        self.assertEqual(log_parser.rows_parsed, 6)
        self.assertEqual(log_parser.header_rows_skipped, 6)
        self.assertEqual(log_parser.rows_inprocessed, 0)
        self.assertEqual(log_parser.latest_valid_date_found, None)
        self.assertEqual(log_parser.latest_valid_refdt_offset_found, 0)
        self.assertEqual(log_parser.first_record_date, None)
        self.assertEqual(log_parser.first_record_refdt_offset, 0)
        self.assertEqual(log_parser.last_record_date, None)
        self.assertEqual(log_parser.last_record_refdt_offset, 0)
        self.assertEqual(log_parser.min_date_inprocessed, None)
        self.assertEqual(log_parser.max_date_inprocessed, None)
        self.assertEqual(
            log_parser.min_refdt_offset_inprocessed,
            csv_parking_log.DEFAULT_END_REFDT_OFFSET - 1
            )
        self.assertEqual(log_parser.max_refdt_offset_inprocessed, 0)
        self.assertEqual(log_parser.records_out_of_date, 0)
        self.assertEqual(log_parser.records_inprocessed, 0)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse_one_record(self):
        '''Test LogParser.parse with a one_record log file.
        '''
        # pylint: disable=protected-access

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_one_record.xlsx'
            )

        log_parser.parse()

        self.assertEqual(log_parser.column_manager.log_version, 'CSVPL16.1')
        self.assertEqual(log_parser.start_date, None)
        self.assertEqual(log_parser.end_date, None)
        self.assertEqual(log_parser.days, None)
        self.assertEqual(log_parser.start_refdt_offset, 0)
        self.assertEqual(
            log_parser.end_refdt_offset,
            csv_parking_log.DEFAULT_END_REFDT_OFFSET
            )
        self.assertEqual(len(log_parser.log_records), 2)
        self.assertEqual(len(log_parser._plate_index), 1)
        self.assertEqual(len(log_parser._canonical_plate_index), 1)
        self.assertEqual(len(log_parser._plate_record_set_index), 1)
        self.assertTrue(log_parser.max_valid_refdt_offset > 0)
        self.assertEqual(log_parser.rows_parsed, 2)
        self.assertEqual(log_parser.header_rows_skipped, 1)
        self.assertEqual(log_parser.rows_inprocessed, 1)
        self.assertEqual(log_parser.latest_valid_date_found, '12.02.16')
        self.assertEqual(log_parser.latest_valid_refdt_offset_found, 6180)
        self.assertEqual(log_parser.first_record_date, '12.01.16')
        self.assertEqual(log_parser.first_record_refdt_offset, 6179)
        self.assertEqual(log_parser.last_record_date, '12.02.16')
        self.assertEqual(log_parser.last_record_refdt_offset, 6180)
        self.assertEqual(log_parser.min_date_inprocessed, '12.01.16')
        self.assertEqual(log_parser.max_date_inprocessed, '12.02.16')
        self.assertEqual(log_parser.min_refdt_offset_inprocessed, 6179)
        self.assertEqual(log_parser.max_refdt_offset_inprocessed, 6180)
        self.assertEqual(log_parser.records_out_of_date, 0)
        self.assertEqual(log_parser.records_inprocessed, 2)

        # logger.info('setting log level to INFO')
        # log_level = logging.DEBUG
        # logger.setLevel(log_level)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse_typemix(self):
        '''Test LogParser.parse with a typemix log file.
        '''
        # pylint: disable=protected-access

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_typemix.xlsx'
            )

        log_parser.parse()

        self.assertEqual(log_parser.column_manager.log_version, 'CSVPL16.1')
        self.assertEqual(log_parser.start_date, None)
        self.assertEqual(log_parser.end_date, None)
        self.assertEqual(log_parser.days, None)
        self.assertEqual(log_parser.start_refdt_offset, 0)
        self.assertEqual(
            log_parser.end_refdt_offset,
            csv_parking_log.DEFAULT_END_REFDT_OFFSET
            )
        self.assertEqual(len(log_parser.log_records), 19)
        self.assertEqual(len(log_parser._plate_index), 8)
        self.assertEqual(len(log_parser._canonical_plate_index), 8)
        self.assertEqual(len(log_parser._plate_record_set_index), 8)
        self.assertTrue(log_parser.max_valid_refdt_offset > 0)
        self.assertEqual(log_parser.rows_parsed, 9)
        self.assertEqual(log_parser.header_rows_skipped, 1)
        self.assertEqual(log_parser.rows_inprocessed, 8)
        self.assertEqual(log_parser.latest_valid_date_found, '12.04.16')
        self.assertEqual(log_parser.latest_valid_refdt_offset_found, 6182)
        self.assertEqual(log_parser.first_record_date, '12.01.16')
        self.assertEqual(log_parser.first_record_refdt_offset, 6179)
        self.assertEqual(log_parser.last_record_date, '12.04.16')
        self.assertEqual(log_parser.last_record_refdt_offset, 6182)
        self.assertEqual(log_parser.min_date_inprocessed, '12.01.16')
        self.assertEqual(log_parser.max_date_inprocessed, '12.04.16')
        self.assertEqual(log_parser.min_refdt_offset_inprocessed, 6179)
        self.assertEqual(log_parser.max_refdt_offset_inprocessed, 6182)
        self.assertEqual(log_parser.records_out_of_date, 0)
        self.assertEqual(log_parser.records_inprocessed, 19)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse_30_lines(self):
        '''Test LogParser.parse with a 30_lines log file.
        '''
        # pylint: disable=protected-access

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_30_lines.xlsx'
            )

        log_parser.parse()

        self.assertEqual(log_parser.column_manager.log_version, 'CSVPL16.1')
        self.assertEqual(log_parser.start_date, None)
        self.assertEqual(log_parser.end_date, None)
        self.assertEqual(log_parser.days, None)
        self.assertEqual(log_parser.start_refdt_offset, 0)
        self.assertEqual(
            log_parser.end_refdt_offset,
            csv_parking_log.DEFAULT_END_REFDT_OFFSET
            )
        self.assertEqual(len(log_parser.log_records), 48)
        self.assertEqual(len(log_parser._plate_index), 22)
        self.assertEqual(len(log_parser._canonical_plate_index), 10)
        self.assertEqual(len(log_parser._plate_record_set_index), 10)
        self.assertTrue(log_parser.max_valid_refdt_offset > 0)
        self.assertEqual(log_parser.rows_parsed, 30)
        self.assertEqual(log_parser.header_rows_skipped, 3)
        self.assertEqual(log_parser.rows_inprocessed, 27)
        self.assertEqual(log_parser.latest_valid_date_found, '12.07.16')
        self.assertEqual(log_parser.latest_valid_refdt_offset_found, 6185)
        self.assertEqual(log_parser.first_record_date, '11.19.16')
        self.assertEqual(log_parser.first_record_refdt_offset, 6167)
        self.assertEqual(log_parser.last_record_date, '11.19.20')
        self.assertEqual(log_parser.last_record_refdt_offset, 7628)
        self.assertEqual(log_parser.min_date_inprocessed, '11.19.16')
        self.assertEqual(log_parser.max_date_inprocessed, '11.19.20')
        self.assertEqual(log_parser.min_refdt_offset_inprocessed, 6167)
        self.assertEqual(log_parser.max_refdt_offset_inprocessed, 7628)
        self.assertEqual(log_parser.records_out_of_date, 0)
        self.assertEqual(log_parser.records_inprocessed, 48)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_csv_parking_log_parser_parse_30_lines(self):
        '''Test LogParser.parse with a 30_lines log file.
        '''
        # pylint: disable=protected-access

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_30_lines.xlsx',
            days=6
            )

        log_parser.parse()

        self.assertEqual(log_parser.column_manager.log_version, 'CSVPL16.1')
        self.assertEqual(log_parser.start_date, '2016-12-02')
        self.assertEqual(log_parser.end_date, '2016-12-08')
        self.assertEqual(log_parser.days, 6)
        self.assertEqual(log_parser.start_refdt_offset, 6180)
        self.assertEqual(log_parser.end_refdt_offset, 6186)
        self.assertEqual(len(log_parser.log_records), 9)
        self.assertEqual(len(log_parser._plate_index), 9)
        self.assertEqual(len(log_parser._canonical_plate_index), 4)
        self.assertEqual(len(log_parser._plate_record_set_index), 4)
        self.assertTrue(log_parser.max_valid_refdt_offset > 0)
        self.assertEqual(log_parser.rows_parsed, 30)
        self.assertEqual(log_parser.header_rows_skipped, 3)
        self.assertEqual(log_parser.rows_inprocessed, 27)
        self.assertEqual(log_parser.latest_valid_date_found, '12.07.16')
        self.assertEqual(log_parser.latest_valid_refdt_offset_found, 6185)
        self.assertEqual(log_parser.first_record_date, '12.02.16')
        self.assertEqual(log_parser.first_record_refdt_offset, 6180)
        self.assertEqual(log_parser.last_record_date, '12.07.16')
        self.assertEqual(log_parser.last_record_refdt_offset, 6185)
        self.assertEqual(log_parser.min_date_inprocessed, '11.19.16')
        self.assertEqual(log_parser.max_date_inprocessed, '11.19.20')
        self.assertEqual(log_parser.min_refdt_offset_inprocessed, 6167)
        self.assertEqual(log_parser.max_refdt_offset_inprocessed, 7628)
        self.assertEqual(log_parser.records_out_of_date, 39)
        self.assertEqual(log_parser.records_inprocessed, 48)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    import os
    run_slow_tests = (
        'RUN_SLOW_TESTS' in os.environ and os.environ['RUN_SLOW_TESTS']
        )
    @unittest.skipIf(
        not run_slow_tests, 'Environment variable RUN_SLOW_TESTS is not true'
        )
    def test_csv_parking_log_parser_parse_reaL20170202(self):
        '''Test LogParser.parse with reaL20170202 log file.
        '''
        # pylint: disable=protected-access

        # - - - - - - - - - - - - - - - -
        log_parser = csv_parking_log.LogParser(
            filepath='sample_log_reaL20170202.xlsx'
            )

        log_parser.parse()

        self.assertEqual(log_parser.column_manager.log_version, 'CSVPL16.1')
        self.assertEqual(log_parser.start_date, None)
        self.assertEqual(log_parser.end_date, None)
        self.assertEqual(log_parser.days, None)
        self.assertEqual(log_parser.start_refdt_offset, 0)
        self.assertEqual(
            log_parser.end_refdt_offset,
            csv_parking_log.DEFAULT_END_REFDT_OFFSET
            )
        self.assertEqual(len(log_parser.log_records), 5389)
        self.assertEqual(len(log_parser._plate_index), 1577)
        self.assertEqual(len(log_parser._canonical_plate_index), 1221)
        self.assertEqual(len(log_parser._plate_record_set_index), 1221)
        self.assertTrue(log_parser.max_valid_refdt_offset > 0)
        self.assertEqual(log_parser.rows_parsed, 4438)
        self.assertEqual(log_parser.header_rows_skipped, 166)
        self.assertEqual(log_parser.rows_inprocessed, 4268)
        self.assertEqual(log_parser.latest_valid_date_found, '02.02.17')
        self.assertEqual(log_parser.latest_valid_refdt_offset_found, 6242)
        self.assertEqual(log_parser.first_record_date, '11.13.1')
        self.assertEqual(log_parser.first_record_refdt_offset, 682)
        self.assertEqual(log_parser.last_record_date, '11.19.20')
        self.assertEqual(log_parser.last_record_refdt_offset, 7628)
        self.assertEqual(log_parser.min_date_inprocessed, '11.13.1')
        self.assertEqual(log_parser.max_date_inprocessed, '11.19.20')
        self.assertEqual(log_parser.min_refdt_offset_inprocessed, 682)
        self.assertEqual(log_parser.max_refdt_offset_inprocessed, 7628)
        self.assertEqual(log_parser.records_out_of_date, 0)
        self.assertEqual(log_parser.records_inprocessed, 5389)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
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
