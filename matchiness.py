#!/usr/bin/env python

import argparse
import logging
import os
import re
import sys


FILENAME = os.path.split(__file__)[-1]
BASE_FILENAME = FILENAME[:-3] if FILENAME[-3:] == '.py' else FILENAME

# LOG_HANDLE = '%s_logger' % FILENAME
DEFAULT_LOG_FILE_NAME = '.'.join([BASE_FILENAME, 'log'])
DEFAULT_LOG_PATH = os.path.join(os.getcwd(), DEFAULT_LOG_FILE_NAME)

# Characters that indicate the entry will mess up regular expressions
RESERVED_CHARS = re.compile("\(|\)|\[|\]")

# Minimum length of a string to consider suitable for testing
MIN_LENGTH = 4

# Biggest allowable length difference between two strings to be able to
# consider them equivalents
MAX_SIZE_DIFF = 2

# Number of contiguous characters to replace with a wildcard when
# constructing match tests.
FUZZ_SIZE = 2

# Number of matching tests for a key and a candidate to be considered
# "matchy".
MATCH_THRESHOLD = 1


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
class AlreadyFoundEquivalenceClass(Exception):
    '''Interrupt when constructing unique equivalence class list.
    '''
    pass


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def argument_parser():
    '''
    Define command line arguments.
    '''

    parser = argparse.ArgumentParser(
        description='''
            Find likely transcription errors in a set of strings, and report
            groupings that are likely all supposed to be the same string.
            '''
        )

    parser.add_argument(
        '-l', '--log-path',
        default=DEFAULT_LOG_PATH,
        help='''
            path to desired log file (DEFAULT: %s).
            ''' % DEFAULT_LOG_FILE_NAME
        )

    parser.add_argument(
        '--no-log',
        default=False,
        action='store_true',
        help='''don't write a log file.
            '''
        )

    parser.add_argument(
        '-v', '--verbose',
        dest='verbose',
        default=0,
        action='count',
        help='''show more output.
            '''
        )

    parser.add_argument(
        'input_file',
        metavar="INPUT_FILE",
        # nargs='*',
        help='''path to file containing set of strings to process.
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
    if not args.no_log:
        fhandler = logging.FileHandler(args.log_path)
        fhandler.setLevel(logging.DEBUG)
        fformatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
        fhandler.setFormatter(fformatter)
        logger.addHandler(fhandler)

    # Logging handler for stdout messages.
    shandler = logging.StreamHandler()

    # PICK ONE, DEPENDING ON HOW args.verbose IS HANDLED.
    # shandler.setLevel(logging.DEBUG if args.verbose else logging.WARNING)
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
def get_string_list_from_input_file(input_file, min_length=0):
    '''Read in the strings to process from a file.
    '''

    string_list = set()

    with open(input_file) as fp:

        for line in fp:

            newstring = line.strip()

            if len(newstring) < min_length:
                continue

            if RESERVED_CHARS.search(newstring):
                continue

            string_list.update([newstring])

    return string_list


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def get_match_score(string_list):
    '''
    '''

    # regex_tests will look like
    # { string: [tests for matchiness to string] }
    regex_tests = {
        s: build_regex_tests(s, FUZZ_SIZE) for s in string_list
        }

    # match_score will look like
    # { key: { candidate: # of matches to candidate among regex_tests[key] } }
    match_score = {
        s: {
            t: len(
                [r.match(t) for r in regex_tests[s] if (
                    len(s) >= MIN_LENGTH and
                    len(t) >= MIN_LENGTH and
                    abs(len(s) - len(t)) < MAX_SIZE_DIFF and
                    r.match(t) is not None
                    )]
                )
            for t in string_list
            }
        for s in regex_tests
        }

    # print match_score
    return match_score


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def find_equivalence_classes(string_list):
    '''
    '''

    # # regex_tests will look like
    # # { string: [tests for matchiness to string] }
    # regex_tests = {
    #     s: build_regex_tests(s, FUZZ_SIZE) for s in string_list
    #     }

    # # match_score will look like
    # # {key: {
    # #     candidate: # of matches to candidate among regex_tests[key]
    # #     }}
    # match_score = {
    #     s: {
    #         t: len(
    #             [r.match(t) for r in regex_tests[s] if (
    #                 len(s) >= MIN_LENGTH and
    #                 len(t) >= MIN_LENGTH and
    #                 abs(len(s) - len(t)) < MAX_SIZE_DIFF and
    #                 r.match(t) is not None
    #                 )]
    #             )
    #         for t in string_list
    #         }
    #     for s in regex_tests
    #     }

    match_score = get_match_score(string_list)

    # Iterate over the match_score information and create equivalence
    # classes of strings, where A =~= B <=> A matches B or B matches A.

    # Start with each string only equivalent to itself.
    equivalents = {s: {s} for s in string_list}

    for s in equivalents:
        for t in match_score[s]:
            # If t's score in s's matches is at least MATCH_THRESHOLD,
            # merge their equivalence classes.
            if match_score[s][t] >= MATCH_THRESHOLD:
                # Create the merged set of equivalents.
                merged_equivalents = equivalents[s] | equivalents[t]
                # Assign it to all of their equivalents.
                for e in merged_equivalents:
                    equivalents[e] = merged_equivalents

    equivalence_classes = []
    for s in equivalents:
        # Extract unique equivalence classes.

        try:
            for e in equivalence_classes:
                # By symmetry, we only need check if one member of s's
                # equivalents is already in some known equivalence class.
                if s in e:
                    raise AlreadyFoundEquivalenceClass
            # We checked all current equivalence classes and didn't find a
            # copy of s's equivalents, so we need to add it as a new class.
            equivalence_classes.append(equivalents[s])

        except AlreadyFoundEquivalenceClass:
            # We already have a representative for s - go on to next one.
            continue

    # for s in equivalents:
    #     # Quick print.
    #     if len(equivalents[s]) < 2:
    #         continue
    #     print "%s:\t%s" % (s, ", ".join(equivalents[s]))
    #     # print "%s: %s" % (s, equivalents[s])
    # print

    return equivalence_classes


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def dump(match_score):
    # This is a quick and dirty dump.
    for key in match_score:
        # print "\nTESTING: %s" % key
        for candidate in match_score[key]:
            if match_score[key][candidate] == 0:
                continue
            if candidate == key:
                continue
            print "%s\t%s\t%s" % (
                str(match_score[key][candidate]),
                key,
                candidate
                )


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def build_regex_tests(targetstring, fuzzsize):
    '''
    Given a starting string, return a list of regexes to test for "near
    matches" to the starting string, by replacing fuzzsize characters with
    a global match wildcard.
    If fuzzsize == 3:
    abcdefghijkl
    ^^^
    '''

    def parens_clean(a_string):
        '''Escape parens in a string.
        '''
        return a_string.replace('(', '\(').replace(')', '\)')

    targetlen = len(targetstring)
    assert fuzzsize > 0
    assert fuzzsize <= targetlen, (
        "Target string \"%s\" too short (%s characters required)"
        ) % (targetstring, fuzzsize)

    regex_tests = []
    # for posn in range(targetlen - fuzzsize + 1):
    for posn in map(lambda x: x - 1, range(targetlen - fuzzsize + 3)):
        new_regex_test = re.compile(r'.*'.join([
            parens_clean(targetstring[0:max(posn, 0)]),
            parens_clean(targetstring[posn + 2:targetlen])
            ]))

    for (prefix, suffix) in [
            [
                targetstring[0:max(x, 0)],
                targetstring[x + fuzzsize:len(targetstring)]
                ]
            for x in range(-(fuzzsize - 1), len(targetstring))
            ]:  # pylint: disable=bad-continuation
        new_regex_test = re.compile(
            r'.*'.join([
                parens_clean(prefix),
                parens_clean(suffix)
                ])
            )
        regex_tests.append(new_regex_test)

    return regex_tests


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
def main():
    '''Main program entry point.
    '''

    parser = argument_parser()
    args = parser.parse_args()

    initialize_logging(args)
    log_startup_configuration(args)

    string_list = get_string_list_from_input_file(args.input_file)

    for e in find_equivalence_classes(string_list):
        if len(e) > 1:
            print "\t".join(e)


# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
if __name__ == "__main__":
    main()
