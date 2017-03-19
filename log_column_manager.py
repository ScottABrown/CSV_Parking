'''Manage csv_parking log version and column mapping/meaning.'''


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class ColumnManager(object):
    '''Manage parking log spreadsheet column interpretations.'''

    # Supported parking log versions. "Creekside Village Parking Log
    # <Year implemented>.<subversion>".
    version_header_row = {
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

    version_column_indices = {
        'CSVPL16.1': {
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
            # 'WARNING': None,
            },
        'CSVPL17.1': {
            'MAKE': 0,
            'MODEL': 1,
            'COLOR': 2,
            'LIC': 3,
            'LOCATION': 4,
            'OPEN_PARKING_1': 5,
            'OPEN_PARKING_2': 6,
            'OPEN_PARKING_3': 7,
            'WARNING': 8,
            'TOWDATE': 9,
            'STREET_PARKING_1': 10,
            'TOWDATE_2': 11,
            },
        }

    _possible_record_type_columns = [
        'OPEN_PARKING_1',
        'OPEN_PARKING_2',
        'OPEN_PARKING_3',
        'WARNING',
        'TOWDATE',
        'STREET_PARKING_1',
        'TOWDATE_2',
        ]

    # The log record type, based on the column in which the date is
    # logged.
    _record_type_map = {
        'OPEN_PARKING_1': 'guest_1',
        'OPEN_PARKING_2': 'guest_2',
        'OPEN_PARKING_3': 'guest_3',
        'STREET_PARKING_1': 'street_1',
        'WARNING': 'warning',
        'TOWDATE': 'guest_tow',
        'TOWDATE_2': 'street_tow',
        }

    # supported_log_versions = version_header_row.keys()
    header_row_match_threshold = 2

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def __init__(self):
        '''Initialize a ColumnManager instance.'''

        self.log_version = None
        self.col_indices = None

        # The columns where a date indicates a new log record and
        # tells us the type.
        self.record_type_columns = None
        self.record_type = None

        # Indices for rows of the Code3 parking spreadsheet.
        # This is initialized when we examine the first row.
        self.column_indices = {}

        # The columns where a date indicates a new log record and tell
        # us the type.
        # This is initialized when we examine the first row.
        self.record_type_columns = []

        # The general category of record, used for dashboard
        # indicators.
        self.record_class = {
            'guest_1': 'guest_parking',
            'guest_2': 'guest_parking',
            'guest_3': 'guest_parking',
            'street_1': 'street_parking',
            'warning': 'warning',
            'guest_tow': 'tow',
            'street_tow': 'tow',
            }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def determine_column_map(self, row):
        '''Examine a row and determine what log version we're managing.'''
        self.column_indices = {}
        self.record_type_columns = []

        self.log_version = self.determine_log_version(row)
        self.column_indices = self.version_column_indices[self.log_version]

        self.record_type_columns = [
            self.column_indices[x]
            for x in self._possible_record_type_columns
            if x in self.column_indices
            ]

        self.record_type = {
            self.column_indices[k]: v
            for k, v in self._record_type_map.iteritems()
            if k in self.column_indices
            }

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    @property
    def license_column(self):
        '''Examine a row and determine what log version we're managing.'''
        return self.column_indices['LIC']


    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    @staticmethod
    def determine_log_version(row):
        '''Examine a row and determine what log version we're managing.'''

        for log_version, row_template in (
                ColumnManager.version_header_row.iteritems()
                ):  # pylint: disable=bad-continuation

            if len(row) != len(row_template):
                continue

            matches = True
            for index, cell in enumerate(row):
                if cell.value.strip() != row_template[index]:
                    matches = False
                    break

            if matches is True:
                return log_version

        return None

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    @classmethod
    def is_header_row(cls, row, version=None):
        '''Examine a row and determine if it's a header row.'''

        # Not sure it's worth the cost here.
        # lower_row = map(lambda x: str(x).lower, row)

        # is_header_row == False
        for log_version, row_template in (
                ColumnManager.version_header_row.iteritems()
                ):  # pylint: disable=bad-continuation

            if version and (log_version != version):
                continue

            if len(row) != len(row_template):
                continue

            match_count = 0
            for index, cell in enumerate(row):
                if unicode(cell.value).strip() == row_template[index]:
                    match_count += 1
                if match_count > cls.header_row_match_threshold:
                    return True

        return False

    # # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # @staticmethod
    # def _is_record_row(row):
    #     '''Determine if a row contains at least one license log record.'''
