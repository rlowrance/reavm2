'''utility functions and classes
'''
import collections
import json
import logging
from typing import Dict, List
import os
import pdb
import sys
import unittest


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
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    if config.get('logging_stderr', False):
        handler = logging.StreamHandler(stream=sys.stderr)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    if config.get('logging_stdout', False):
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    if 'logging_filename' in config:
        path = os.path.join(config['dir_working'], config['logging_filename'])
        handler = logging.FileHandler(path)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger


def log_config(module_name: str, config: Dict, logger) -> None:
    'log configuration keys and values'
    for k in sorted(config.keys()):
        if not k.startswith('_'):
            v = config[k]
            if isinstance(v, list):
                for i, item in enumerate(v):
                    logger.info('config key %s[%d] value %s' % (k, i, item))
            else:
                logger.info('config key %s value %s' % (k, config[k]))


class TestMakeLogger(unittest.TestCase):
    def test(self):
        pdb.set_trace()
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
