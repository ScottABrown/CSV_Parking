
'''Test cases for the log_column_manager.py module.'''

import unittest

from log_column_manager import ColumnManager


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
class TestColumnManager(unittest.TestCase):
    '''Basic test cases for ColumnManager.'''

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def setUp(self):
        '''Test case common fixture setup.'''
        pass

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_log_column_manager_init(self):
        '''Test initialization of a ColumnManager instance.'''

        column_manager = ColumnManager()
        self.assertIsNotNone(column_manager)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class TestColumnManagerRowAnalysis(unittest.TestCase):
    '''Test cases for ColumnManager.determine_log_version().'''

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    @classmethod
    def setUpClass(cls):
        '''Class level common fixture definitions.'''

        cls.header_row = {
            'CSVPL16.1': [
                "MAKE", "MODEL", "COLOR", "LIC#", "LOCATION",
                "1ST (96 HR) OPEN PARKING", "2nd", "3rd",
                "DATE VEHICLE WAS TOWED",
                "1ST (24 HR) STREET PARKING",
                "DATE VEHICLE WAS TOWED",
                ],
            'CSVPL17.1': [
                "MAKE", "MODEL", "COLOR", "LIC#", "LOCATION",
                "1ST (96 HR) OPEN PARKING", "2nd", "3rd",
                "CONFIRM WARNING TAG DATE:", "DATE VEHICLE WAS TOWED",
                "1ST (24 HR) STREET PARKING",
                "DATE VEHICLE WAS TOWED",
                ]
            }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_determine_log_version(self):
        '''Basic test cases for ColumnManager.determine_log_version().'''

        # pylint: disable=protected-access

        for version, row in self.header_row.iteritems():
            self.assertEqual(
                ColumnManager.determine_log_version(row),
                version
                )

        self.assertIsNone(ColumnManager.determine_log_version([]))
        self.assertIsNone(
            ColumnManager.determine_log_version(['a', 'b', 'c'])
            )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_is_header_row(self):
        '''Basic test cases for ColumnManager.is_header_row().'''

        # pylint: disable=protected-access

        column_manager = ColumnManager()

        version = 'CSVPL16.1'
        not_version = 'CSVPL17.1'
        row = self.header_row[version]
        self.assertTrue(column_manager.is_header_row(row))
        self.assertTrue(
            column_manager.is_header_row(row, version=version)
            )
        self.assertFalse(
            column_manager.is_header_row(row, version=not_version)
            )

        not_version = 'CSVPL16.1'
        version = 'CSVPL17.1'
        row = self.header_row[version]
        self.assertTrue(column_manager.is_header_row(row))
        self.assertTrue(
            column_manager.is_header_row(row, version=version)
            )
        self.assertFalse(
            column_manager.is_header_row(row, version=not_version)
            )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_is_record_row(self, version=None):
        '''Basic test cases for ColumnManager._is_record_row().'''

        # pylint: disable=protected-access

        column_manager = ColumnManager()


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def test_determine_column_map(self):
        '''Basic test cases for ColumnManager.determine_column_map().'''

        # pylint: disable=protected-access

        column_manager = ColumnManager()

        for version, row in self.header_row.iteritems():
            column_manager.determine_column_map(row)
            # self.assertFalse(
            #     column_manager.determine_column_map(row, version=version)
            #     )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Define test suite.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# pylint: disable=invalid-name
load_case = unittest.TestLoader().loadTestsFromTestCase
all_suites = {
    # Lowercase these for pylint/flake8.
    'suite_ColumnManager': load_case(
        TestColumnManager
        ),
    }

master_suite = unittest.TestSuite(all_suites.values())
# pylint: enable=invalid-name

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == '__main__':
    unittest.main()
