'''utility functions and classes
'''
import collections
import datetime
import json
import logging
from typing import Dict, List
import os
import pdb
import sys
import unittest


class Error(Exception):
    '''Base class for application-specific errors'''
    pass


class InputError(Error):
    '''Error in input file'''
    def __init__(self, reason, detail):
        self.reason = reason
        self.detail = detail


class NotFoundError(Error):
    '''key was not found'''
    def __init__(self, value):
        self.value = value


class NotUnique(Error):
    '''key leads to more than one value'''
    def __init__(self, value):
        self.value = value


def as_date(s: str) -> datetime.date:
    '''Convert string to a datetime or raise an error

    Convert day 0 to day 1

    The call to datetime.date() will raise an error, if the date is not in the calendar
    '''
    if '-' in s:
        # assume format is YYYY-MM-DD
        year, month, day = s.split('-')
        return datetime.date(int(year), int(month), int(day) if day != '00' else 1)
    else:
        # assume format is YYYYMMDD
        day = int(s[6:8])
        return datetime.date(int(s[0:4]), int(s[4:6]), day if day > 0 else 1)


def best_apn(formatted: str, unformatted: str) -> int:
    '''return as int, the best of the APN values, or raise ValueError if neither is usable.'''
    # attempt to use the unformatted value
    try:
        # some unformatted values are decorated with space, _, or - characters
        return int(unformatted.replace(' ', '').replace('_', '').replace('-', ''))
    except ValueError:
        pass

    # attempt to use the formatted value
    try:
        value = int(formatted.replace('-', ''))
        return value
    except ValueError:
        pass

    # check a case that occured at least once in the deeds file
    if formatted == '' and unformatted == '':
        raise ValueError

    # code this way so that during development, I could find other strange encodings
    pdb.set_trace()
    raise ValueError


def parse_invocation_arguments(argv: List[str]) -> Dict[str, any]:
    '''Parse invocation aguments

    Args:
    - argv[0]: name used to invoke program
    - argv[1:]: path to config file, which is a text file encoded with JSON or abc=value (an override)

    Returns:
       A collection.ChainMap(overrides, first_config, second_config, ...)
       containing the parsed config file values and overrides.
    '''
    maps = []
    overrides = {}
    for arg in argv[1:]:
        try:
            with open(arg, 'r') as f:
                maps.append(json.load(f))
        except FileNotFoundError:
            # process as an override
            splits = arg.split('=')
            if len(splits) != 2:
                print('invocation argument not filename nor key=value: %s' % arg)
                sys.exit(1)
            key, value = splits
            try:
                overrides[key] = int(value)
                continue
            except ValueError:
                pass
            try:
                overrides[key] = float(value)
                continue
            except ValueError:
                pass
            try:
                overrides[key] = eval(value)
                continue
            except NameError:
                pass
            overrides[key] = value  # value must be a str
    result = collections.ChainMap(overrides, *maps)
    return result


def make_logger(module_name, config):
    '''Setup logging to print and write to files

    CONFIG KEYS USED
    - logging_filename: optional name of file in dir_working to write to
    - logging_level:    one of DEBUG INFO WARNING ERROR CRITICAL
    - logging_stderr:   optional; if True, write log messages to stderr
    - logging_stdout:   optional: if True, write log messages to stdout
    '''
    logger = logging.getLogger(module_name)
    level = config['logging_level'].upper()
    logger.setLevel(
        logging.DEBUG if level == 'DEBUG' else
        logging.INFO if level == 'INFO' else
        logging.WARNING if level == 'WARNING' else
        logging.ERROR if level == 'ERROR' else
        logging.CRITICAL if level == 'CRITICAL' else
        None
        )
    formatter_long = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    formatter_short = logging.Formatter('%(levelname)s %(message)s')
    if config.get('logging_stderr', False):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setFormatter(formatter_short)
        logger.addHandler(handler)
    if config.get('logging_stdout', False):
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter_short)
        logger.addHandler(handler)
    if 'logging_filename' in config:
        path = os.path.join(config['dir_working'], config['logging_filename'])
        handler = logging.FileHandler(path)
        handler.setFormatter(formatter_long)
        logger.addHandler(handler)
    return logger


def log_config(module_name: str, config: Dict, logger) -> None:
    'log configuration keys and values'
    message_template = 'config\n key %s\n value %s'
    for k in sorted(config.keys()):
        if not k.startswith('_'):
            v = config[k]
            if isinstance(v, list):
                for i, item in enumerate(v):
                    key = '%s[%d]' % (k, i)
                    logger.info(message_template % (key, item))
            else:
                logger.info(message_template % (k, config[k]))


class TestAsDate(unittest.TestCase):
    def test_1(self):
        d = as_date('1994-12-11')
        self.assertEqual(d.year, 1994)
        self.assertEqual(d.month, 12)
        self.assertEqual(d.day, 11)

    def test_2(self):
        d = as_date('19941211')
        self.assertEqual(d.year, 1994)
        self.assertEqual(d.month, 12)
        self.assertEqual(d.day, 11)

    def test_3(self):
        try:
            as_date('')
            self.fail('should have raised an error')
        except Exception:
            self.assertTrue(True)


class TestMakeLogger(unittest.TestCase):
    def test(self):
        config = {
            'loggin_stderr': False,
            'logging_stdout': True,
            'logging_level': 'info',
            }
        logger = make_logger('utility.py', config)
        logger.debug('debug message')
        logger.info('info message')
        logger.warning('warning message')
        logger.error('error message')
        logger.critical('critical message')


class TestParseInvocationArguments(unittest.TestCase):
    def setUp(self):
        'write the test config file'
        self.file_name = 'utility_test.json'
        d = {'a': 123, 'b': 23.0, 'c': "abc", 'd': [1, 11.0, 'abc']}
        with open(self.file_name, 'w') as f:
            json.dump(d, f)

    def tearDown(self):
        'remove the test config file'
        os.remove(self.file_name)

    def test_no_config(self):
        r = parse_invocation_arguments(['xx'])
        self.assertTrue(len(r) == 0)

    def test_just_config(self):
        r = parse_invocation_arguments(['xx', 'utility_test.json'])
        self.assertTrue(len(r) == 4)
        self.assertEqual(r['a'], 123)
        self.assertEqual(r['b'], 23.0)
        self.assertEqual(r['c'], "abc")
        self.assertEqual(r['d'], [1, 11.0, 'abc'])

    def test_with_override(self):
        r = parse_invocation_arguments(['xx', 'utility_test.json', "a=1", "b=17.0", "c=xyz", "xxx=999"])
        self.assertTrue(len(r) == 5)
        self.assertEqual(r['a'], 1)
        self.assertEqual(r['b'], 17.0)
        self.assertEqual(r['c'], "xyz")
        self.assertEqual(r['d'], [1, 11.0, 'abc'])
        self.assertEqual(r['xxx'], 999)


if __name__ == '__main__':
    unittest.main()
