'''Read raw files and create SQLITE3 data base with transactions split
into training and testing

INVOCATION EXAMPLES
===================

python etl.py etl_config.json config.json


CONFIGURATION FILE
==================

A text file containing a JSON object containing these fields
- "dir_data": path to data directoy
- "dir_project": path with dir_data to project-specific files
- "in_deeds": [paths with dir_data to deeds zip files]
- "in_taxrolls": [path within dir_data to taxroll zip files]
- "in_census": path within dir_data to census file
- "in_geocoding": path with dir_data to geocoding file
- "out_feature_vectors": path with dir_data to


Each deed and taxroll zip file contains one file. That file is a CSV
file in tab-separated format.
'''
import pdb
import sys

import utility as u


def main(argv):
    pdb.set_trace()
    config = u.parse_invocation_arguments(argv)
    logger = u.make_logger(argv[0], config)
    u.log_config(argv[0], config, logger)


if __name__ == '__main__':
    main(sys.argv)
