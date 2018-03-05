'''Read raw files and create SQLITE3 data base with transactions split
into training and testing.

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
import collections
import csv
import datetime
import os
import pdb
import pprint
import sqlite3
import subprocess
import sys
import typing

import utility as u


def lookup_code(conn, table_name, code_table, description):
    '''Return code as str or raise u.NotFoundError'''
    stmt = 'SELECT value FROM %s where code_table == "%s" AND description == "%s"' % (
        table_name,
        code_table,
        description,
        )
    n_rows = 0
    for row in conn.execute(stmt):
        value = row['value']
        n_rows += 1
    if n_rows == 0:
        raise u.NotFoundError((table_name, code_table, description))
    elif n_rows == 1:
        return value
    else:
        raise u.NotUnique((table_name, code_table, description))


def read_codes(conn, config, logger, table_name):
    '''Create table codes_deeds from info in config['in_codes_deeds']'''
    def skip_code(table_name, row):
        '''Return True iff the code should be skipped

        These codes are incorrect for this application
        '''
        if table_name == 'codes_deeds':
            if row['CODE TABLE'] == 'DEEDC':
                if row['DESCRIPTION'] == 'LIS PENDENS - NON CALIFORNIA':
                    # all of our deeds are for california
                    return True
        elif table_name == 'codes_taxrolls':
            pass
        else:
            raise ValueError('bad table_name: %s' % table_name)
        return False

    stmt_drop = 'DROP TABLE IF EXISTS %s' % table_name
    conn.execute(stmt_drop)

    stmt_create = '''CREATE TABLE %s
    ( code_table    text NOT NULL
    , value         text NOT NULL
    , description   text NOT NULL
    , PRIMARY KEY (code_table, value, description)
    )
    ''' % table_name
    conn.execute(stmt_create)

    path = os.path.join(config['dir_data'], config['in_' + table_name])
    with open(path, encoding='latin-1') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row_index, row in enumerate(reader):
            pprint.pprint(row)
            if skip_code(table_name, row):
                logger.warning('skipping code: %s %s' % (table_name, str(row)))
                continue

            if False:
                print('existing values')
                for existing in conn.execute('SELECT * from %s' % table_name):
                    print(tuple(existing))

            # Some rows are duplicated in the taxrolls code table
            # This code handles that by skipping code-description values already in the code table
            try:
                stmt = 'INSERT INTO %s VALUES("%s", "%s", "%s")' % (
                    table_name,
                    row['CODE TABLE'],
                    row['VALUE'],
                    row['DESCRIPTION'],
                )
                conn.execute(stmt)
            except sqlite3.IntegrityError as err:
                if str(err).startswith('UNIQUE constraint failed:'):
                    existing_code = lookup_code(conn, table_name, row['CODE TABLE'], row['DESCRIPTION'])
                    if existing_code == row['VALUE']:
                        # there is a duplicate record in the csv file
                        pass
                    else:
                        print('fix this problem')
                        pdb.set_trace()
                else:
                    # unexpected error
                    raise err
    return


def read_codes_deeds(conn, config, logger):
    '''Create table codes_deeds'''
    return read_codes(conn, config, logger, 'codes_deeds')


def read_codes_taxrolls(conn, config, logger):
    '''Create table codes_deeds'''
    return read_codes(conn, config, logger, 'codes_taxrolls')


def read_deeds(conn, config, logger):
    '''Create table deeds from data in deeds zip files'''
    sale_amounts = collections.defaultdict(list)
    date_census_became_known = u.as_date(config['date_census_became_known'])
    max_sale_amount = float(config['max_sale_amount'])

    code_single_family_residence = lookup_code(conn, 'codes_deeds', 'PROPN', 'Single Family Residence / Townhouse')
    code_grant_deed = lookup_code(conn, 'codes_deeds', 'DEEDC', 'GRANT DEED')
    code_arms_length = lookup_code(conn, 'codes_deeds', 'PRICATCODE', 'ARMS LENGTH TRANSACTION')
    code_resale = lookup_code(conn, 'codes_deeds', 'TRNTP', 'RESALE')
    code_new_construction = lookup_code(conn, 'codes_deeds', 'TRNTP', 'SUBDIVISION/NEW CONSTRUCTION')
    # code_sale_confirmed = lookup_code(conn, 'codes_deeds', 'SCODE', 'CONFIRMED')
    # code_sale_verified = lookup_code(conn, 'codes_deeds', 'SCODE', 'VERIFIED')
    code_sale_full_price = lookup_code(conn, 'codes_deeds', 'SCODE', 'SALE PRICE (FULL)')

    def make_values(row: dict) -> typing.List:
        'return list of values or raise ValueError if the row is not valid'
        if False:
            pprint.pprint(row)
        # make sure deed is one that we want
        if not row['PROPERTY INDICATOR CODE'] == code_single_family_residence:
            raise u.InputError('not a single family residence', row['PROPERTY INDICATOR CODE'])

        if not row['DOCUMENT TYPE CODE'] == code_grant_deed:
            raise u.InputError('deed not a grant deed', row['DOCUMENT TYPE CODE'])

        if not row['PRI CAT CODE'] == code_arms_length:
            raise u.InputError('deed not an arms-length transaction', row['PRI CAT CODE'])

        # Assume that if the MULTI APLN FLAG CODE is missing, then one APN was sold
        if (not row['MULTI APN FLAG CODE'] == '') or int(row['MULTI APN COUNT']) > 1:
            raise u.InputError('deed has multipe APNs', (row['MULTI APN FLAG CODE'], row['MULTI APN COUNT']))

        # these codes are truncated in the file (leading zeros are omitted)
        try:
            ttc = int(row['TRANSACTION TYPE CODE'])
        except ValueError:
            raise u.InputError('TRANSACTION TYPE CODE not an int', row['TRANSACTION TYPE CODE'])

        if ttc in (int(code_resale), int(code_new_construction)):
            pass
        else:
            raise u.InputError('deed not resale nor new construction', row['TRANSACTION TYPE CODE'])

        # Version 1 accepted sale_code_confirmed and sale_code_verified as well
        # However, we don't know that the confirmations and verifications were for a transaction with full price
        if row['SALE CODE'] == code_sale_full_price:
            pass  # full price
        else:
            raise u.InputError('deed not full price', row['SALE CODE'])

        # build a list of valid values in the order defined by the create statement
        values = []
        try:
            sale_date = u.as_date(row['SALE DATE'])
        except Exception:
            # NOTE: instead of giving up, one could impute the sale date from the recording date
            # The sale date is about 2 months before the recording date
            # The difference can be measured
            raise u.InputError('invalid SALE DATE', row['SALE DATE'])

        if sale_date < date_census_became_known:
            raise u.InputError('sale date before date census became known', row['SALE DATE'])
        else:
            values.append(sale_date)

        try:
            apn = u.best_apn(row['APN FORMATTED'], row['APN UNFORMATTED'])
        except Exception:
            raise u.InputError('invalid APN', (row['APN FORMATTED'], row['APN UNFORMATTED']))

        values.append(apn)

        try:
            sale_amount = float(row['SALE AMOUNT'])
        except Exception:
            raise u.InputError('invalid SALE AMOUNT', row['SALE AMOUNT'])

        if sale_amount <= 0:
            raise u.InputError('SALE AMOUNT not positive', sale_amount)
        elif sale_amount > max_sale_amount:
            raise u.InputError('SALE AMOUNT exceed maximum sale amount', sale_amount)
        else:
            values.append(sale_amount)

        key = (apn, sale_date)
        sale_amounts[key].append(sale_amount)
        if len(sale_amounts[key]) > 1:
            raise u.InputError('multiple deed sale amounts', str((key, sale_amount)))
        else:
            return values

    conn.execute('DROP TABLE IF EXISTS deeds')
    conn.execute(
        '''CREATE TABLE deeds
        ( apn         integer NOT NULL
        , sale_date   date    NOT NULL
        , sale_amount real    NOT NULL
        , PRIMARY KEY (apn, sale_date)
        )
        '''
        )
    counter = collections.Counter()
    error_reasons = collections.Counter()
    debug = False
    for zipfilename in config['in_deeds']:
        # inflate the zip files directly because reading the archive members led to unicode issues
        # and to having to read the entire file into memory
        if debug:
            if not zipfilename.endswith('F1.zip'):
                print('DEBUG: skipping', zipfilename)
                continue
        path_zip = os.path.join(config['dir_data'], zipfilename)
        cp_unzip = subprocess.run([
            'unzip',
            '-o',       # overwrite existing inflated file
            '-dtmp',   # unzip into /tmp in the current directory (which holds the source code)
            path_zip,
            ])

        assert cp_unzip.returncode == 0
        filename = path_zip.split('/')[-1].split('.')[0]
        path_txt = os.path.join(os.getcwd(), 'tmp', filename + '.txt')
        # path_txt = os.path.join('tmp', filename + '.txt')
        with open(path_txt, encoding='latin-1') as csvfile:
            # NOTE: When reading ... F3.txt, error raised: _csv.Error: field larger than field limit (131072)
            # ref: https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
            # Hyp: the problem is that the file contains a quoting char in one of the tab-delimited fields
            reader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row_index, row in enumerate(reader):
                if debug:
                    print(row_index)
                try:
                    values = make_values(row)
                    conn.execute('INSERT INTO deeds VALUES (?, ?, ?)', values)
                    counter['saved'] += 1
                except u.InputError as err:
                    logger.warning('deed file %s record %d InputError %s' % (path_zip, row_index + 1, err))
                    counter['skipped'] += 1
                    error_reasons[err.reason] += 1
                if debug:
                    if len(sale_amounts) > 100:
                        break
        logger.info('read all deeds from %s' % path_zip)
        cp_rm = subprocess.run(['rm', path_txt])
        assert cp_rm.returncode == 0
    print('read all deeds zipfiles')

    # delete deeds records where an APN has multiple valid deeds on same date with different prices.
    # NOTE: View the log files to see that most of the time, those multiple deeds have the same price.
    for apn_date, prices in sale_amounts.items():
        if len(prices) > 1:
            logger.warning('duplicate sale amounts for deed %s' % str(apn_date))
            for price in prices:
                logger.warning('  duplicate price        %f' % price)
            if len(set(prices)) == 1:
                continue  # all prices the same
            stmt = 'DELETE FROM deeds WHERE apn = %s AND sale_date = %s' % (apn_date[0], apn_date[1])
            conn.execute(stmt)
            logger.warning('deed record deleted')
            counter['saved then deleted'] += 1

    # Summarize results
    for k, v in counter.items():
        logger.info('deeds counter %30s = %7d' % (k, v))
    for k in sorted(error_reasons.keys()):
        logger.info('deeds input error %7d x %s' % (error_reasons[k], k))
    return


def read_taxrolls(conn, config, logger):
    '''Create table parcels from data in taxroll zip files'''
    pdb.set_trace()
    # TODO: revist these data structures
    sale_amounts = collections.defaultdict(list)
    date_census_became_known = u.as_date(config['date_census_became_known'])
    max_sale_amount = float(config['max_sale_amount'])

    def make_values(row: dict) -> (bool, typing.List):
        'return list of values or raise ValueError if the row is not valid'
        if True:
            pprint.pprint(row)

        pdb.set_trace()
        try:
            apn = u.best_apn(row['APN FORMATTED'], row['APN UNFORMATTED'])
        except Exception:
            raise u.InputError('invalid APN', (row['APN FORMATTED'], row['APN UNFORMATTED']))

        try:
            value = row['PROPERTY INDICATOR CODE']
            property_indicator_code = int(value)
        except Exception:
            raise u.InputError('property_indicator_code not an int', value)

        try:
            value = row['CENSUS TRACT']
            census_tract = int(value)
        except Exception:
            raise u.InputError('census_trace is not an int', value)

        try:
            value = row['PROPERTY ZIPCODE']
            property_zipcode_9 = int(value)
        except Exception:
            raise u.InputError('property_zipcode_9 not an int', value)

        try:
            value = row['PROPERTY ZIPCODE'][0:5]
            property_zipcode_5 = int(value)
        except Exception:
            raise u.InputError('property_zipcode_5 not an int', value)

        try:
            value = row['PROPERTY INDICATOR CODE']
            property_indicator_code = int(value)
        except Exception:
            raise u.InputError('property_indicator_code is not an int', value)

        if property_indicator_code != 10:
            # not a single family residence
            return (False, (apn,
                            property_indicator_code,
                            census_tract,
                            property_zipcode_5,
                            property_zipcode_9,
                            ))

        pdb.set_trace()
        # TODO: create fields specific to single family residences

        # OLD BELOW ME
        # make sure deed is one that we want
        if not row['DOCUMENT TYPE CODE'] == 'G':
            raise u.InputError('deed not a grant deed', row['DOCUMENT TYPE CODE'])

        if not row['PRI CAT CODE'] == 'A':
            raise u.InputError('deed not an arms-length transaction', row['PRI CAT CODE'])

        if (not row['MULTI APN FLAG CODE'] == '') or int(row['MULTI APN COUNT']) > 1:
            raise u.InputError('deed has multipe APNs', (row['MULTI APN FLAG CODE'], row['MULTI APN COUNT']))

        if row['TRANSACTION TYPE CODE'] == '1':
            pass  # resale
        else:
            if row['TRANSACTION TYPE CODE'] == '3':
                pass  # new construction
            else:
                raise u.InputError('deed not resale nor new construction', row['TRANSACTION TYPE CODE'])

        if row['SALE CODE'] == 'C':
            pass  # confirmed (assumed to be full price)
        elif row['SALE CODE'] == 'V':
            pass  # verified (assume to be be full price)
        elif row['SALE CODE'] == 'F':
            pass  # full price
        else:
            raise u.InputError('deed not full price', row['SALE CODE'])

        # build a list of valid values in the order defined by the create statement
        values = []
        try:
            sale_date = u.as_date(row['SALE DATE'])
        except Exception:
            # NOTE: instead of giving up, one could impute the sale date from the recording date
            # The sale date is about 2 months before the recording date
            # The difference can be measured
            raise u.InputError('invalid SALE DATE', row['SALE DATE'])

        if sale_date < date_census_became_known:
            raise u.InputError('sale date before date census became known', row['SALE DATE'])
        else:
            values.append(sale_date)

        try:
            apn = u.best_apn(row['APN FORMATTED'], row['APN UNFORMATTED'])
        except Exception:
            raise u.InputError('invalid APN', (row['APN FORMATTED'], row['APN UNFORMATTED']))

        values.append(apn)

        try:
            sale_amount = float(row['SALE AMOUNT'])
        except Exception:
            raise u.InputError('invalid SALE AMOUNT', row['SALE AMOUNT'])

        if sale_amount <= 0:
            raise u.InputError('SALE AMOUNT not positive', sale_amount)
        elif sale_amount > max_sale_amount:
            raise u.InputError('SALE AMOUNT exceed maximum sale amount', sale_amount)
        else:
            values.append(sale_amount)

        key = (apn, sale_date)
        sale_amounts[key].append(sale_amount)
        if len(sale_amounts[key]) > 1:
            raise u.InputError('multiple deed sale amounts', str((key, sale_amount)))
        else:
            return values

    pdb.set_trace()
    conn.execute('DROP TABLE IF EXISTS deeds')
    conn.execute(
        '''CREATE TABLE parcels
        ( apn                          integer NOT NULL
        , property_indicator_code      integer NOT NULL
        , census_tract                 integer NOT NULL
        , property_zipcode_5           integer NOT NULL
        , property_zipcode_9           integer NOT NULL
        , improvement_value_calculated real
        , land_value_calculated        real
        , effective_year_built         integer
        , number_of_buildings          integer
        , total_rooms                  integer
        , units_number                 integer
        , property_indicator_code      text
        , land_square_footage          real
        , universal_land_use_code      text
        , living_square_feet           real
        , year_built                   integer
        , PRIMARY KEY (apn)
        )
        '''
        )
    pdb.set_trace()
    counter = collections.Counter()
    error_reasons = collections.Counter()
    debug = False
    for zipfilename in config['in_taxrolls']:
        # inflate the zip files directly because reading the archive members led to unicode issues
        # and to having to read the entire file into memory
        if debug:
            if not zipfilename.endswith('F1.zip'):
                print('DEBUG: skipping', zipfilename)
                continue
        path_zip = os.path.join(config['dir_data'], zipfilename)
        cp_unzip = subprocess.run([
            'unzip',
            '-o',       # overwrite existing inflated file
            '-dtmp',   # unzip into /tmp in the current directory (which holds the source code)
            path_zip,
            ])

        assert cp_unzip.returncode == 0
        filename = path_zip.split('/')[-1].split('.')[0]
        path_txt = os.path.join(os.getcwd(), 'tmp', filename + '.txt')
        # path_txt = os.path.join('tmp', filename + '.txt')
        with open(path_txt, encoding='latin-1') as csvfile:
            # NOTE: When reading ... F3.txt, error raised: _csv.Error: field larger than field limit (131072)
            # ref: https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
            # Hyp: the problem is that the file contains a quoting char in one of the tab-delimited fields
            reader = csv.DictReader(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
            for row_index, row in enumerate(reader):
                if debug:
                    print(row_index)
                if False and row_index == 255716:
                    print('found row_index', row_index)
                    pdb.set_trace()
                try:
                    is_single_family_residence, values = make_values(row)
                    conn.execute('INSERT INTO deeds VALUES (?, ?, ?)', values)
                    counter['saved'] += 1
                except u.InputError as err:
                    logger.warning('deed file %s record %d InputError %s' % (path_zip, row_index + 1, err))
                    counter['skipped'] += 1
                    error_reasons[err.reason] += 1
                if debug:
                    if len(sale_amounts) > 100:
                        break
        logger.info('read all deeds from %s' % path_zip)
        cp_rm = subprocess.run(['rm', path_txt])
        assert cp_rm.returncode == 0
    print('read all deeds zipfiles')

    # delete deeds records where an APN has multiple valid deeds on same date with different prices.
    # NOTE: View the log files to see that most of the time, those multiple deeds have the same price.
    for apn_date, prices in sale_amounts.items():
        if len(prices) > 1:
            logger.warning('duplicate sale amounts for deed %s' % str(apn_date))
            for price in prices:
                logger.warning('  duplicate price        %f' % price)
            if len(set(prices)) == 1:
                continue  # all prices the same
            stmt = 'DELETE FROM deeds WHERE apn = %s AND sale_date = %s' % (apn_date[0], apn_date[1])
            conn.execute(stmt)
            logger.warning('deed record deleted')
            counter['saved then deleted'] += 1

    # Summarize results
    for k, v in counter.items():
        logger.info('deeds counter %30s = %7d' % (k, v))
    for k in sorted(error_reasons.keys()):
        logger.info('deeds input error %7d x %s' % (error_reasons[k], k))
    pass


def main(argv):
    pdb.set_trace()
    config = u.parse_invocation_arguments(argv)
    logger = u.make_logger(argv[0], config)
    u.log_config(argv[0], config, logger)

    # for date and datetime fields, see https://docs.python.org/3/library/sqlite3.html#default-adapters-and-converters
    conn = sqlite3.connect(
        os.path.join(config['dir_working'], config['out_db']),
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
    conn.row_factory = sqlite3.Row

    if True:
        read_codes_deeds(conn, config, logger)
        read_codes_taxrolls(conn, config, logger)
    if True:
        read_deeds(conn, config, logger)
    if False:
        read_taxrolls(conn, config, logger)
        read_census(conn, config, logger)

        create_transactions(conn)

        delete_intermediate_tables(conn)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main(sys.argv)
