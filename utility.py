'''utility functions and classes
'''
import json
from typing import Dict, List
import os
import pdb
import unittest


def parse_invocation_arguments(argv: List[str]) -> Dict[str, any]:
    '''Parse invocation aguments

    Args:
    - argv[0]: name used to invoke program
    - argv[1]: path to config file, which is a text file encoded with JSON
    - remaining optional args: abc=value. This overrides the key "abc" in the config file

    Returns:
       A dictionary containing the parsed values and overrides.
    '''
    if len(argv) < 2:
        return dict()
    with open(argv[1], 'r') as f:
        result = json.load(f)

    if len(argv) > 2:
        for arg in argv[2:]:
            key, value = arg.split('=')
            try:
                result[key] = int(value)
                continue
            except ValueError:
                pass
            try:
                result[key] = float(value)
                continue
            except ValueError:
                pass
            try:
                result[key] = eval(value)
                continue
            except NameError:
                pass
            result[key] = value  # value must be a str

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
