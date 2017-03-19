'''CLasses for working with Creekside Village parking logs.'''

from datetime import datetime
from datetime import timedelta
import json
import logging
# Set default logging handler to avoid "No handler found" warnings.
try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        '''Placeholder handler.'''
        def emit(self, record):
            pass
import os
import re

import xlrd

from log_column_manager import ColumnManager
import matchiness

logging.getLogger(__name__).addHandler(NullHandler())

# Date comparison is easier when we use "days since ref date" to compare.
REF_DATETIME = datetime(2000, 01, 01)

# This is the Excel vslue for offset to January 1, 2000.
EXCEL_OFFSET_TO_REF_DATETIME = 36526

# The name of the key for the days-since-REF_DATETIME, so we encode the
# REF_DATETIME in the key.
REF_DATETIME_KEY = 'days_since_{}'.format(REF_DATETIME.strftime('%Y%m%d'))

DEFAULT_START_REFDT_OFFSET = 0
DEFAULT_END_REFDT_OFFSET = 2 ** 16  # ~180 years in the future.

STANDARD_DATE_FORMAT = '%Y-%m-%d'
LOG_DATE_FORMAT = '%m.%d.%y'
FILENAME_DATE_FORMAT = '%Y%m%d'

# The number of days per block for summarizing total log entries for
# a plate.
WINDOW_DAYS = 30
WINDOWS = {
    'log1-long': WINDOW_DAYS * 3,
    'log1-medium': WINDOW_DAYS * 2,
    'log1-short': WINDOW_DAYS * 1,
    'log5-long': WINDOW_DAYS * 3,
    'log5-medium': WINDOW_DAYS * 2,
    'log5-short': WINDOW_DAYS * 1,
    }


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _most_common_element(a_list):
    '''Find the most common element of a list.'''
    counts = [(x, a_list.count(x)) for x in set(a_list)]
    max_count = max([c[1] for c in counts])
    return [c[0] for c in counts if c[1] == max_count][0]


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _force_float_to_int(record_row, column_num):
    '''Convert float type to int.

    Some spreadsheet fields, if numeric, are floats in Excel but we
    want to treat them as ints (mainly to avoid a spurious '.' being
    added after, say, a plate value).
    '''
    try:
        record_row[column_num].value = (
            int(float(record_row[column_num].value))
            )
    except (ValueError, OverflowError):
        pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _log_date_to_datetime(log_date):
    '''Convert a parking log date to a datetime object.
    '''
    log_date_pattern = (
        r'(?P<log_mon>\d{1,2})\.(?P<log_day>\d{1,2})\.(?P<log_year>\d{1,2})'
        )

    # Handle both the m.dd.yy dot separated format and Excel date value.
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

        return datetime.strptime(log_date, LOG_DATE_FORMAT)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _to_yyyy_mm_dd(date_rep, date_format=None):
    '''Convert a datetime, refdt_offset or string date to YYYY-MM-DD.

    Arguments:

        date_rep (datetime.datetime, int or str):
            The value to be converted to YYYY-MM-DD.

        date_format (str):
            A date date_format specification matching a string
            date_rep.

    '''
    # Convert any date_rep to a datetime.
    converter = {
        datetime: lambda x: x,
        int: _refdt_offset_to_datetime,
        str: lambda x: datetime.strptime(x, date_format)
        }
    as_datetime = converter[type(date_rep)](date_rep)
    return as_datetime.strftime(STANDARD_DATE_FORMAT)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _refdt_offset_to_datetime(offset, refdate=REF_DATETIME):
    '''Return the datetime for refdate plus offset days.
    '''
    return refdate + timedelta(days=offset)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _datetime_to_refdt_offset(end_dt, refdate=REF_DATETIME):
    '''Return the number of days from refdate to the datetime end_dt.
    '''
    return (end_dt - refdate).days


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _log_date_to_refdt_offset(log_date, refdate=REF_DATETIME):
    '''Return the number of days from refdate to the log date string log_date.
    '''
    return _datetime_to_refdt_offset(_log_date_to_datetime(log_date), refdate)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _get_latest_valid_refdt_offset(filename):
    '''Try to find the latest refdt offset that should be considered valid.

    If a date is present in the filename, we assume this is the date
    the file was generated, so any offset to a date past this
    shouldn't be a valid parking logging case.

    If we don't find a date in the filename, we use today.
    '''
    logger = logging.getLogger(__name__)

    filename_pieces = filename.split('.')
    refdt_values = []

    for piece in filename_pieces:
        try:
            dt_value = datetime.strptime(piece, FILENAME_DATE_FORMAT)
            refdt_values.append(_datetime_to_refdt_offset(dt_value))
        except ValueError:
            pass

    # We really expect at most one refdt value in the filename.
    if len(refdt_values) > 1:
        logger.warn(
            'multiple refdt values found in filename: %s', refdt_values
            )

    if len(refdt_values) == 0:
        return _datetime_to_refdt_offset(datetime.now())
    else:
        return max(refdt_values)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def _get_five_day_totals(plate_record_sets):
    '''Find five day totals for a canonical plate's plate record sets.'''
    # The plate record sets are returned sorted by
    # refdt_offset.
    index_a = 0
    for index_b, plate_record_set in enumerate(plate_record_sets):
        # Advance index_a until the selected record is within
        # five days of index_b.
        while (
                plate_record_sets[index_b].refdt_offset - 5 >=
                plate_record_sets[index_a].refdt_offset
                ):  # pylint: disable=bad-continuation
            index_a += 1
        # We've now moved index_a up until the days between the
        # records at index_a and index_b is no more than 5.
        # plate_record_set.five_day_total = index_b - index_a + 1

        if not plate_record_set.record_class['guest_parking']:
            continue
        # Add one because the record set at index_b isn't included in
        # the range, but we know it's a guest parking record set.
        plate_record_set.five_day_total = len([
            r for r in plate_record_sets[index_a:index_b]
            if r.record_class['guest_parking']
            ]) + 1


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class CsvParkingLogError(Exception):
    '''Base class for module errors. '''
    def __init__(self, msg, code=2):
        self.msg = msg
        self.code = code


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class CsvParkingLogContentError(Exception):
    '''An unexpected data element was encountered. '''
    def __init__(self, msg, code=3):
        self.msg = msg
        self.code = code


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class CsvParkingLogFormatError(Exception):
    '''An unexpected data format was encountered. '''
    def __init__(self, msg, code=4):
        self.msg = msg
        self.code = code


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class CsvParkingLogStructureError(Exception):
    '''An unexpected workbook structure was encountered. '''
    def __init__(self, msg, code=5):
        self.msg = msg
        self.code = code


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class LogRecord(object):
    '''A representation of one log record entry for a vehicle.

    The ``LogRecord`` class represents one instance of a vehicle being
    logged in the Creekside Village parking log. This includes the plate,
    make, model, date logged and the type of log (e.g. first, second,
    third guest parking, tow record, fire lane, etc.)
    '''

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # pylint: disable=too-many-arguments
    def __init__(
            self,
            plate, date, record_type,
            make=None, model=None, color=None, location=None
            ):  # pylint: disable=bad-continuation
        '''Initialize one LogRecord instance.'''
        self.plate = unicode(plate)
        self.date = unicode(date)
        self.record_type = unicode(record_type)
        self.make = unicode(make)
        self.model = unicode(model)
        self.color = unicode(color)
        self.location = unicode(location)
        self.refdt_offset = _datetime_to_refdt_offset(
            _log_date_to_datetime(self.date)
            )

        # We'll calculate these and assign later.
        self.canonical_plate = None

        logger_name = '%s.%s' % (__name__, self.__class__.__name__)
        self._logger = logging.getLogger(logger_name)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def to_dict(self):
        '''Return instance representation as a dictionary.'''

        return {
            'plate': self.plate,
            'canonical_plate': self.canonical_plate,
            'date': self.date,
            'record_type': self.record_type,
            'make': self.make,
            'model': self.model,
            'color': self.color,
            'location': self.location,
            REF_DATETIME_KEY: self.refdt_offset,
            }


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class PlateRecordSet(object):
    '''A collection of log records for a plate on a single date.

    The ``PlateRecordSet`` class groups a collection of plate records
    on a common date, managing cases where the same plate might appear
    more than once due to a number of circumstances:

        *   A tow record as well as a log record.
        *   Mistranscription errors in transferring data to Excel.
        *   Canonicalization collisions between what should be
            separated plates.
    '''

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def __init__(self, column_manager, records):
        '''Initialize one PlateRecordSet instance.'''

        logger_name = '%s.%s' % (__name__, self.__class__.__name__)
        self._logger = logging.getLogger(logger_name)

        self.column_manager = column_manager
        self.log_records = records

        self.canonical_plate = None
        self.date = None
        self.refdt_offset = None
        self.record_class = {
            'guest_parking': False,
            'street_parking': False,
            'tow': False
            }

        # This will be set after all PlateRecordSets are
        # constructed.
        self.five_day_total = 0

        self._extract_canonical_plate(records)
        self._extract_date(records)  # This also sets refdt_offset.
        self._extract_record_class(records)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _extract_canonical_plate(self, records):
        '''Set canonical_plate after ensuring it is unique across records.'''

        canonical_plates = set([r.canonical_plate for r in records])

        if len(canonical_plates) > 1:
            err_msg = (
                'multiple canonical plates found when initializing'
                'PlateRecordSet: %s'
                )
            self._logger.error(err_msg, list(canonical_plates))
            raise ValueError(err_msg % list(canonical_plates))

        self.canonical_plate = list(canonical_plates)[0]

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _extract_date(self, records):
        '''Set refdt_offset after ensuring it is unique across records.'''

        refdt_offsets = set([r.refdt_offset for r in records])

        if len(refdt_offsets) > 1:
            err_msg = (
                'multiple dates found when initializing'
                'PlateRecordSet: %s'
                )
            self._logger.error(err_msg, list(refdt_offsets))
            raise ValueError(err_msg % list(refdt_offsets))

        self.refdt_offset = list(refdt_offsets)[0]
        self.date = records[0].date

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _extract_record_class(self, records):
        '''Mark the record classes found in this group.'''

        for record in records:
            self.record_class[
                self.column_manager.record_class[record.record_type]
                ] = True

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def to_dict(self):
        '''Return instance representation as a dictionary.'''

        return {
            'canonical_plate': self.canonical_plate,
            'date': self.date,
            REF_DATETIME_KEY: self.refdt_offset,
            'record_class': self.record_class,
            'log_records': [record.to_dict() for record in self.log_records],
            'five_day_total': self.five_day_total,
            }


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class LogParser(object):
    '''A Creekside Village parking log reader and parser.

    A LogParser is responsible for reading in basic records and keeping
    track of the date offsets found.

    Arguments:

        filepath (str):
            The path to the log file the ``LogParser`` instance will
            parse.

        start_date (str, optional):
            The date, in YYYY-MM-DD format, of the earliest day to
            include in the records resulting from parsing.

        end_date (str, optional):
            The date, in YYYY-MM-DD format, of the day *after* the
            last day to include in the records resulting from parsing.

        days (int, optional):
            The maximum number of days to include in the records
            resulting from parsing.

    Raises:

        ValueError: if all three of ``start_date``, ``end_date`` and
        ``days`` are passed. At most two are necessary.

    The records retained after parsing will only include those with a
    date on or after ``start_date`` and before (not on) ``end_date``.
    If not explicitly provided, these date boundaries will be
    inferred according to the following rules.

    *   If the ``days`` parameter is provided along with one of
        ``start_date`` and ``end_date``, the other date boundary will
        be calculated.

    *   If the ``days`` parameter is provided and neither
        ``start_date`` nor ``end_date`` is provided, ``end_date`` will
        be set to the latest valid date found for a parsed record.

    '''

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def __init__(
            self,
            filepath,
            start_date=None,
            end_date=None,
            days=None
            ):  # pylint: disable=bad-continuation
        '''Initialize one LogRecord instance.'''

        logger_name = '%s.%s' % (__name__, self.__class__.__name__)
        self._logger = logging.getLogger(logger_name)

        self.column_manager = None

        # The path to the log file.
        self.filepath = filepath

        # We set the "real" values in _initialize_refdt_offset_boundaries().
        # Note that internally we do all comparisons using
        # refdt_offset values, so start_date and end_date need not be
        # set.
        self.start_date = start_date
        self.end_date = end_date
        self.days = days
        self.start_refdt_offset = DEFAULT_START_REFDT_OFFSET
        self.end_refdt_offset = DEFAULT_END_REFDT_OFFSET

        # The following checks that if days is defined, at least one
        # of start_date and end_date is not defined; if one of them is
        # defined, this also calculates the other by
        # adding/subtracting days as appropriate.
        self._initialize_refdt_offset_boundaries()

        # Individual LogRecord instances creeated from the log.
        self.log_records = []

        # An index, by plate, of all log records for that plte.
        self._plate_index = {}

        # An index, by canonical plate, of all log records for that
        # canonical plate.
        self._canonical_plate_index = {}

        # An index, by canonical plate, of all plate record sets for
        # a given canonical plate. Each plate record set contains all
        # log records for the canonical plate on a given day.
        self._plate_record_set_index = {}

        # Set the offset from REF_DATETIME to the latest date that is
        # considered valid. Typically either a date determined from
        # the name of the log file (if found) or today.
        self.max_valid_refdt_offset = _get_latest_valid_refdt_offset(
            os.path.basename(filepath)
            )

        # Parsing statistics.
        self.rows_parsed = 0
        self.header_rows_skipped = 0
        self.rows_inprocessed = 0

        # Record statistics updated as the file is parsed.
        # - - - - - - - - - - - - - - - -
        # Records with dates that are invalid will still be
        # inprocessed, but if we are dynamically calculating start and
        # end dates based on the value of self.days, we'll use the
        # latest valid date found as the end date and invalid date
        # records will not be included.
        self.latest_valid_date_found = None
        self.latest_valid_refdt_offset_found = 0

        # The first and last record dates found, among record dates
        # that fall on or after self.start_date and before
        # self.end_date.
        self.first_record_date = None
        self.first_record_refdt_offset = 0

        self.last_record_date = None
        self.last_record_refdt_offset = 0

        # The minimum date and refdt_offset among all records
        # inprocessed, even if the record was outside the desired date
        # boundaries.
        self.min_date_inprocessed = None
        self.max_date_inprocessed = None

        # We start with a high min and low max, and increase/decrease
        # as we process records. This is obvious once you think about
        # it, but it always makes me do a double take when I see it.
        self.min_refdt_offset_inprocessed = DEFAULT_END_REFDT_OFFSET - 1
        self.max_refdt_offset_inprocessed = 0

        # The number of records that were found outside the desired
        # date boundaries. These are not inprocessed.
        self.records_out_of_date = 0

        # The total log entries inprocessed, that is, turned into
        # saved records.
        self.records_inprocessed = 0

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _initialize_refdt_offset_boundaries(self):
        '''Calculate refdt_offset boundaries.
        '''

        # assert self.start_refdt_offset == DEFAULT_START_REFDT_OFFSET
        if self.start_date and self.end_date and self.days:
            err_msg = (
                'no more than two of start_date (%s), end_date (%s)'
                ' and days (%s) can be defined.'
                )
            self._logger.error(
                err_msg, self.start_date, self.end_date, self.days
                )
            raise ValueError(
                err_msg % (self.start_date, self.end_date, self.days)
                )

        self._logger.debug('calculating reference date offsets...')
        if self.start_date:
            self.start_refdt_offset = _datetime_to_refdt_offset(
                datetime.strptime(self.start_date, STANDARD_DATE_FORMAT)
                )

        if self.end_date:
            self.end_refdt_offset = _datetime_to_refdt_offset(
                datetime.strptime(self.end_date, STANDARD_DATE_FORMAT)
                )

        # assert self.start_refdt_offset == DEFAULT_START_REFDT_OFFSET
        if self.days and (self.start_date or self.end_date):
            if self.start_date:
                assert self.end_refdt_offset == DEFAULT_END_REFDT_OFFSET
                self.end_refdt_offset = self.start_refdt_offset + self.days
                assert self.end_date is None
                self.end_date = _to_yyyy_mm_dd(self.end_refdt_offset)
            else:
                assert self.start_refdt_offset == DEFAULT_START_REFDT_OFFSET
                self.start_refdt_offset = self.end_refdt_offset - self.days
                assert self.start_date is None
                self.start_date = _to_yyyy_mm_dd(self.start_refdt_offset)

        self._logger.debug('starting offset: %s', self.start_refdt_offset)
        self._logger.debug('ending offset: %s', self.end_refdt_offset)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _validate_refdt_offset(self, log_record):
        '''Assess refdt offset validity and update tracking bounds.

        In addition to updating the latest valid, minimum and maximum
        improcessed values for date and refdt_offset, this method
        logs a warning for invalid dates and returns True or False
        depending on whether the record's refdt offset is within the
        start and end specified boundaries.

        '''
        if log_record.refdt_offset > self.max_valid_refdt_offset:
            self._logger.warn(
                'warning: row %s: refdt %s exceeds limit %s, date was %s',
                self.rows_parsed,
                log_record.refdt_offset,
                self.max_valid_refdt_offset,
                log_record.date
                )
        elif log_record.refdt_offset > self.latest_valid_refdt_offset_found:
            # It's valid, so it's the new latest found.
            self.latest_valid_refdt_offset_found = log_record.refdt_offset
            self.latest_valid_date_found = log_record.date

        # - - - - - - - - - - - - - - - -
        if log_record.refdt_offset < self.min_refdt_offset_inprocessed:
            self.min_refdt_offset_inprocessed = log_record.refdt_offset
            self.min_date_inprocessed = log_record.date

        if log_record.refdt_offset > self.max_refdt_offset_inprocessed:
            self.max_refdt_offset_inprocessed = log_record.refdt_offset
            self.max_date_inprocessed = log_record.date

        # if (
        #         log_record.refdt_offset < self.start_refdt_offset
        #         or log_record.refdt_offset >= self.end_refdt_offset
        #         ):  # pylint: disable=bad-continuation
        if not self._check_offset_in_bounds(log_record):
            self.records_out_of_date += 1
            return False

        return True

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _check_offset_in_bounds(self, log_record):
        '''Check whether a log record's refdt_offset is between offset bounds.

        Note that the lower bound is included, the upper excluded.
        '''
        return (
            log_record.refdt_offset >= self.start_refdt_offset
            and log_record.refdt_offset < self.end_refdt_offset
            )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _update_validated_record_bounds(self, log_record):
        '''Update the records of the first and last record date.

        These are the first and last dates among log entries that are
        being accepted and turned into parsed records.

        '''
        # logger = logging.getLogger(__name__)
        # if log_record.refdt_offset == 7628:
        #     logger.warning('we hit it...')
        if (
                not self.first_record_refdt_offset
                or log_record.refdt_offset < self.first_record_refdt_offset
                ):  # pylint: disable=bad-continuation
            self.first_record_date = log_record.date
            self.first_record_refdt_offset = log_record.refdt_offset

        if (
                not self.last_record_refdt_offset
                or log_record.refdt_offset > self.last_record_refdt_offset
                ):  # pylint: disable=bad-continuation
            self.last_record_date = log_record.date
            self.last_record_refdt_offset = log_record.refdt_offset

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _prune_to_dynamic_date_bounds(self):
        '''Discard records outside date bounds based only on days.

        This method updates all the output data fields affected by
        the pruning.
        '''
        self._logger.info('pruning to dynamic date bounds...')

        assert self.start_date is None
        assert self.end_date is None
        assert self.start_refdt_offset == DEFAULT_START_REFDT_OFFSET
        assert self.end_refdt_offset == DEFAULT_END_REFDT_OFFSET
        assert self.days is not None

        self._set_dynamic_date_bounds()

        # We have to calcualate these anew, so we reset them here.
        self.first_record_date = None
        self.first_record_refdt_offset = 0
        self.last_record_date = None
        self.last_record_refdt_offset = 0
        self._plate_index = {}

        # Comprehension selector that also offsets necessary records.
        def in_bounds(log_record):
            '''Check offset bounds, and tally those out of bounds.'''
            if not self._check_offset_in_bounds(log_record):
                self.records_out_of_date += 1
                return False
            self._update_validated_record_bounds(log_record)
            return True

        # Remove log_record entries that are out of bounds.
        self.log_records = [r for r in self.log_records if in_bounds(r)]

        # Add to plate index.
        for log_record in self.log_records:
            _ = self._plate_index.setdefault(log_record.plate, [])
            self._plate_index[log_record.plate].append(log_record)

        self._logger.debug(
            'first record offset set to: %s', self.first_record_refdt_offset
            )
        self._logger.debug(
            'last record offset set to: %s', self.last_record_refdt_offset
            )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _set_dynamic_date_bounds(self):
        '''Calculate start/end date if only days is set.

        We want days worth of records and the last record we retain is
        on the day *before* end_refdt_offset. Therefore we set the
        end_refdt_offset to one more than the last record we'll be
        retaining.

        '''
        assert self.days and not (self.start_date or self.end_date)
        self._logger.debug(
            'resetting date offset bounds for %s days...', self.days
            )

        # Previously we ran through this block even if we had already set
        # end_date, so we might have had a lower last_record_refdt_offset
        # than latest_valid_date_found. Now we only pass here if there was
        # no end_date, so we didn't throw any records away.
        # self.end_refdt_offset = min(
        #     self.last_record_refdt_offset,
        #     self.latest_valid_refdt_offset_found
        #     ) + 1
        assert (
            self.last_record_refdt_offset >=
            self.latest_valid_refdt_offset_found
            )

        self.end_refdt_offset = self.latest_valid_refdt_offset_found + 1
        # self.last_record_refdt_offset = self.end_refdt_offset
        self.end_date = _to_yyyy_mm_dd(self.end_refdt_offset)

        self.start_refdt_offset = self.end_refdt_offset - self.days
        self.start_date = _to_yyyy_mm_dd(self.start_refdt_offset)

        self._logger.debug(
            'starting offset set to: %s', self.start_refdt_offset
            )
        # self._logger.debug(
        #     'first record offset set to: %s', self.first_record_refdt_offset
        #     )
        self._logger.debug('start date set to: %s', self.start_date)

        self._logger.debug(
            'ending offset set to: %s', self.end_refdt_offset
            )
        # self._logger.debug(
        #     'last record offset set to: %s', self.last_record_refdt_offset
        #     )
        self._logger.debug('end date set to: %s', self.end_date)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _canonicalize_plates(self):
        '''Find canonical plates and create canonical plate index.

        The canonical representative will be the most commonly
        occurring plate among records that are equivalent as
        determined by matchiness.

        The lists of log records that are the values of the
        canonical plate index are sorted by ``refdt_offset``.

        '''

        matches = matchiness.find_equivalence_classes(
            self._plate_index.keys()
            )

        # print '--------'
        for plate_list in matches:
            # Get all log records with a plate in matches.
            matching_records = []
            for plate in plate_list:
                matching_records.extend(self._plate_index[plate])

            canonical_plate = _most_common_element(
                [r.plate for r in matching_records]
                )

            # print [r.plate for r in matching_records]
            for log_record in matching_records:
                log_record.canonical_plate = canonical_plate
                # print '{}\t{}'.format(
                #     log_record.plate, log_record.canonical_plate
                #     )
            self._canonical_plate_index[canonical_plate] = sorted(
                matching_records, key=lambda x: x.refdt_offset
                )

        # print '-----------'
        # print len(self.log_records)
        # print sum([len(x) for x in self._plate_index.values()])
        # print sum([len(x) for x in self._canonical_plate_index.values()])
        # print '-----------'

        # exit()

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _consolidate_date_records(self):
        '''Combine multiple records for the same date without information loss.

        Multiple records may be recorded on one date for the same
        canonical plate under a number of circumstances:
        *   A tow record as well as a log record.
        *   Mistranscription errors in transferring data to Excel.
        *   Canonicalization collisions between what should be separated
            plates.


        '''

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def _log_parse_statistics(self):
        '''Log statistics on the parsed data.'''

        self._logger.debug(
            'excel rows processed: %s',
            self.rows_inprocessed
            )
        self._logger.debug(
            'header rows skipped: %s',
            self.header_rows_skipped
            )

        self._logger.debug(
            'earliest refdt_offset found: %s',
            self.min_refdt_offset_inprocessed
            )
        self._logger.debug(
            'earliest date found: %s',
            self.min_date_inprocessed
            )
        self._logger.debug(
            'latest refdt_offset found: %s',
            self.max_refdt_offset_inprocessed
            )
        self._logger.debug(
            'latest date found: %s',
            self.max_date_inprocessed
            )
        self._logger.debug(
            'latest valid refdt_offset found: %s',
            self.latest_valid_refdt_offset_found
            )
        self._logger.debug(
            'latest valid date found: %s',
            self.latest_valid_date_found
            )

        self._logger.debug(
            'records inprocessed: %s',
            self.records_inprocessed
            )
        self._logger.debug(
            'out of date records skipped: %s',
            self.records_out_of_date
            )
        self._logger.debug(
            'records retained: %s',
            len(self.log_records)
            )

        # self._logger.debug(
        #     'plates found: %s', len(plates)
        #     )
        # for index_type in ['LIC', 'MAKE', 'MODEL']:
        #     self._logger.debug(
        #         'records in %s: %s',
        #         index_type, len(record_index[index_type])
        #         )

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    # pylint: disable=invalid-name
    def _calculate_guest_parking_window_totals(self, plate):
        '''Populate the 30, 60, 90 day window totals for a canonical plate.'''

        record_sets = self._plate_record_set_index[plate]
        # We tally with an index, and convert to a list of
        # {key:, value:} dicts at the end.
        window_totals = {k: 0 for k in WINDOWS}

        # record_sets is sorted in refdt_offset order.
        for record_set in record_sets:
            if not record_set.record_class['guest_parking']:
                continue
            days_since_record = self.end_refdt_offset - record_set.refdt_offset
            for window_type, window_size in WINDOWS.iteritems():
                if days_since_record <= window_size:
                    if window_type[:4] == 'log1':
                        window_totals[window_type] += 1
                    elif (
                            window_type[:4] == 'log5' and
                            record_set.five_day_total >= 3
                            ):  # pylint: disable=bad-continuation
                        window_totals[window_type] += 1

        return [{'key': k, 'value': v} for k, v in window_totals.iteritems()]
    # pylint: enable=invalid-name

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def parse(self):
        '''Parse the instance's parking log file.'''

        self._logger.debug('parsing log file %s', self.filepath)
        workbook = xlrd.open_workbook(self.filepath)

        self.column_manager = ColumnManager()

        sheet = workbook.sheet_by_name('Sheet1')
        self.column_manager.determine_column_map(sheet.row(0))

        if self.column_manager.log_version is None:
            err_msg = '%s: sheet %s row 0 is not a recognized header row'
            self._logger.error(err_msg, self.filepath, sheet.name)
            raise CsvParkingLogStructureError(
                err_msg % (self.filepath, sheet.name)
                )
        self._logger.info(
            'log version determined: %s', self.column_manager.log_version
            )

        # if not _is_header_row(sheet.row(0)):
        #     err_msg = '%s: sheet %s row 0 is not a header row'
        #     self._logger.error(err_msg, self.filepath, sheet.name)
        #     raise CsvParkingLogStructureError(
        #         err_msg % (self.filepath, sheet.name)
        #         )

        number_of_rows = sheet.nrows
        self._logger.debug('rows: %s', number_of_rows)

        # Just to be sure these are reset.
        self.rows_parsed = 0
        self.header_rows_skipped = 0
        self.rows_inprocessed = 0

        license_column = self.column_manager.license_column

        for row_num in range(number_of_rows):
            self.rows_parsed += 1
            record_row = sheet.row(row_num)

            # if not record_row[COL_INDICES['LIC']].value:
            if not record_row[license_column].value:
                continue

            # if _is_header_row(record_row):
            if self.column_manager.is_header_row(record_row):
                self.header_rows_skipped += 1
                continue

            self.rows_inprocessed += 1
            _force_float_to_int(
                record_row, self.column_manager.column_indices['LIC']
                )
            _force_float_to_int(
                record_row, self.column_manager.column_indices['MODEL']
                )

            self.create_row_records(record_row)

        self._log_parse_statistics()

        if self.days and not (self.start_date or self.end_date):
            # This will also dynamically calculate start and end dates.
            self._prune_to_dynamic_date_bounds()

        self._canonicalize_plates()
        self._log_parse_statistics()

        for plate, log_records in self._canonical_plate_index.iteritems():
            plate_record_sets = self.get_plate_record_sets(log_records)
            self._plate_record_set_index[plate] = plate_record_sets
            _get_five_day_totals(plate_record_sets)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def create_row_records(self, record_row):
        '''Create LogRecord instances for the dates logged in this row.
        '''

        # Syntax sugar.
        column_indices = self.column_manager.column_indices

        plate = record_row[column_indices['LIC']].value

        # Add a record for each of these potential date fields
        # that have a value defined.
        for event_field_index in self.column_manager.record_type_columns:

            record_type = self.column_manager.record_type[event_field_index]

            # If a value is present for this type of event, it should
            # be the date the event was logged.
            if record_row[event_field_index].value:

                record_date = record_row[event_field_index].value

                new_record = LogRecord(
                    plate,
                    record_date,
                    record_type,
                    make=record_row[column_indices['MAKE']].value,
                    model=record_row[column_indices['MODEL']].value,
                    color=record_row[column_indices['COLOR']].value,
                    location=record_row[column_indices['LOCATION']].value
                    )

                if self._validate_refdt_offset(new_record):
                    self._update_validated_record_bounds(new_record)
                    self.log_records.append(new_record)
                    self.records_inprocessed += 1
                    _ = self._plate_index.setdefault(
                        new_record.plate, []
                        )
                    self._plate_index[new_record.plate].append(new_record)

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def get_plate_record_sets(self, canonical_plate_log_records):
        '''Create a list of PlateRecordSet instances from log records.

        Arguments:

            canonical_plate_log_records (list):
                A list of ``LogRecord`` instances that share a common
                ``canonical_plate`` value.

        The list of plate record sets returned will be sorted by
        ``refdt_offset``.

        '''

        records = sorted(
            canonical_plate_log_records,
            key=lambda r: r.refdt_offset
            )
        canonical_plates = set([r.canonical_plate for r in records])

        if len(canonical_plates) > 1:
            err_msg = (
                'get_plate_record_sets():'
                ' canonical_plate must be unique; found: %s'
                )
            self._logger.error(err_msg, list(canonical_plates))
            raise ValueError(err_msg % list(canonical_plates))

        plate_record_sets = []

        # Walk through records, finding groups with common
        # refdt_offset. Since we sorted, this is straightforward.
        record_num = 0
        while record_num < len(records):

            current_refdt_offset = records[record_num].refdt_offset
            current_record_set = []

            while(
                    record_num < len(records) and
                    records[record_num].refdt_offset == current_refdt_offset
                    ):  # pylint: disable=bad-continuation

                current_record_set.append(records[record_num])
                record_num += 1

            new_set = PlateRecordSet(
                self.column_manager, current_record_set
                )
            plate_record_sets.append(new_set)

        return plate_record_sets

    # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    def dashboard_data(self):
        '''Create a structure with data for the dashboard.
        {
            "date_range"
                "first_record_date",
                "first_record_refdt_offset",
                "last_record_date",
                "last_record_refdt_offset"
            "records_by_lic"
                [PLATE]
                    "canonical_lic",
                    "lic_equivalents",
                    "records",
                        []
                            "canonical_lic",
                            "days_since_20000101",
                            "five_day_total",
                            "lic_equivalents",
                            "raw_color",
                            "raw_date",
                            "raw_lic",
                            "raw_location",
                            "raw_make",
                            "raw_model"
                    "window_total"
                        []
                            "key", "value"
            }
        '''

        dashboard_data = {
            'date_range': {
                'first_record_date': self.first_record_date,
                'first_record_refdt_offset': self.first_record_refdt_offset,
                'last_record_date': self.last_record_date,
                'last_record_refdt_offset': self.last_record_refdt_offset,
                },
            'records_by_lic': {
                plate: {
                    'canonical_plate': (
                        self._plate_record_set_index[plate][0].canonical_plate
                        ),
                    'records': [u.to_dict() for u in v],
                    'window_total': (
                        self._calculate_guest_parking_window_totals(plate)
                        ),
                    }
                for plate, v in self._plate_record_set_index.iteritems()
                }
            }

        # This may be consumed downstream.
        return dashboard_data
