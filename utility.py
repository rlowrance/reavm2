'''utility functions and classes
'''
import collections
import json
from typing import Dict, List
import os
import pdb
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
            key, value = arg.split('=')
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
    pdb.set_trace()
    result = collections.ChainMap(overrides, *maps)
    return result


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
