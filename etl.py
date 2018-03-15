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
import numpy as np
import os
import pdb
import pprint
import random
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
    debug = False

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
            if debug:
                pprint.pprint(row)
            if skip_code(table_name, row):
                logger.warning('skipping code: %s %s' % (table_name, str(row)))
                continue

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


class Deed:
    def __init__(self, conn, config, logger):
        def get_code(table_name, description):
            return lookup_code(conn, 'codes_deeds', table_name, description)

        self.conn = conn
        self.config = config
        self.logger = logger

        # constraining values
        self.date_census_became_known = u.as_date(config['date_census_became_known'])[0]
        self.max_sale_amount = float(config['max_sale_amount'])

        # codes in the deeds files
        self.code_single_family_residence = get_code('PROPN', 'Single Family Residence / Townhouse')
        self.code_grant_deed = get_code('DEEDC', 'GRANT DEED')
        self.code_arms_length = get_code('PRICATCODE', 'ARMS LENGTH TRANSACTION')
        self.code_resale = get_code('TRNTP', 'RESALE')
        self.code_new_construction = get_code('TRNTP', 'SUBDIVISION/NEW CONSTRUCTION')
        self.code_sale_full_price = get_code('SCODE', 'SALE PRICE (FULL)')

        self.sale_amounts = {}  # key = (apn, sale_date)  value = sale_amount
        self.sale_date_day_0_converted_to_1 = 0

    def accumulate(self, row):
        '''Mutate self.features or raise u.InputError'''

        # make sure deed is one that we want
        if not row['PROPERTY INDICATOR CODE'] == self.code_single_family_residence:
            raise u.InputError('not a single family residence', row['PROPERTY INDICATOR CODE'])

        if not row['DOCUMENT TYPE CODE'] == self.code_grant_deed:
            raise u.InputError('deed not a grant deed', row['DOCUMENT TYPE CODE'])

        if not row['PRI CAT CODE'] == self.code_arms_length:
            raise u.InputError('deed not an arms-length transaction', row['PRI CAT CODE'])

        # Assume that if the MULTI APLN FLAG CODE is missing, then one APN was sold
        if (not row['MULTI APN FLAG CODE'] == '') or int(row['MULTI APN COUNT']) > 1:
            raise u.InputError('deed has multipe APNs', (row['MULTI APN FLAG CODE'], row['MULTI APN COUNT']))

        try:
            ttc = int(row['TRANSACTION TYPE CODE'])  # convert to int, since leading zeroes are omitted in file
        except ValueError:
            raise u.InputError('TRANSACTION TYPE CODE not an int', row['TRANSACTION TYPE CODE'])

        if ttc in (int(self.code_resale), int(self.code_new_construction)):
            pass
        else:
            raise u.InputError('deed not resale nor new construction', row['TRANSACTION TYPE CODE'])

        # Version 1 accepted sale_code_confirmed and sale_code_verified as well
        # However, we don't know that the confirmations and verifications were for a transaction with full price
        # So this version accepts fewer deeds with the hope that those accepted are more accurate.
        if row['SALE CODE'] == self.code_sale_full_price:
            pass  # full price
        else:
            raise u.InputError('deed not full price', row['SALE CODE'])

        # attempt to extract the feature values
        try:
            sale_date, e = u.as_date(row['SALE DATE'])
            if e == '0 to 1':
                self.sale_date_day_0_converted_to_1 += 1
        except Exception:
            # Earlier versions of this program imputed the sale date from the recording date
            # This version prefers more accurate sales dates rather than more sale amounts
            raise u.InputError('invalid SALE DATE', row['SALE DATE'])

        if sale_date < self.date_census_became_known:
            raise u.InputError('sale date before date census became known', row['SALE DATE'])

        try:
            apn = u.best_apn(row['APN FORMATTED'], row['APN UNFORMATTED'])
        except Exception:
            raise u.InputError('invalid APN', (row['APN FORMATTED'], row['APN UNFORMATTED']))

        try:
            sale_amount = float(row['SALE AMOUNT'])
        except Exception:
            raise u.InputError('invalid SALE AMOUNT', row['SALE AMOUNT'])

        if sale_amount <= 0:
            raise u.InputError('SALE AMOUNT not positive', sale_amount)
        if sale_amount > self.max_sale_amount:
            raise u.InputError('SALE AMOUNT exceed maximum sale amount', sale_amount)

        key = (apn, sale_date)
        if key in self.sale_amounts:
            if self.sale_amounts[key] != sale_amount:
                # possible one of the extra sale amounts is a correction
                # but this program doesn't try to handle that condidtion
                raise u.InputError('multiple deed sale amounts', str((key, sale_amount, self.sale_amounts[key])))
        else:
            self.sale_amounts[key] = sale_amount

    def log_summary(self):
        self.logger.info('%d sale dates with day 0 converted to day 1' % self.sale_date_day_0_converted_to_1)
        pass

    def create_table(self):
        debug = False
        self.conn.execute('DROP TABLE IF EXISTS deeds')
        self.conn.execute(
            '''CREATE TABLE deeds
            ( apn         integer NOT NULL
            , sale_date   date    NOT NULL
            , sale_year   integer NOT NULL
            , sale_month  integer NOT NULL
            , sale_day    integer NOT NULL
            , sale_amount real    NOT NULL
            , PRIMARY KEY (apn, sale_date)
            )
            '''
        )

        for key, sale_amount in self.sale_amounts.items():
            apn, sale_date = key
            self.conn.execute(
                'INSERT INTO deeds VALUES (?, ?, ?, ?, ?, ?)',
                (apn,
                 sale_date,
                 sale_date.year,
                 sale_date.month,
                 sale_date.day,
                 sale_amount,
                ))
        if debug:
            print('first 10 rows in table deeds')
            pdb.set_trace()
            for row in self.conn.execute('SELECT * FROM deeds LIMIT 10'):
                pprint.pprint(dict(row))
            pdb.set_trace()


def read_deeds(conn, config, logger):
    '''Create table deeds from data in deeds zip files'''
    debug = False

    counter = collections.Counter()
    error_reasons = collections.Counter()
    deed = Deed(conn, config, logger)
    for zipfilename in config['in_deeds']:
        # inflate the zip files directly because reading the archive members led to unicode issues
        # and to having to read the entire file into memory
        if debug:
            if not zipfilename.endswith('F7.zip'):
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
                if debug and False:
                    print(row_index)
                    pprint.pprint(row)
                try:
                    deed.accumulate(row)
                    counter['accumulated'] += 1
                except u.InputError as err:
                    # logger.warning('deed file %s record %d InputError %s' % (path_zip, row_index + 1, err))
                    counter['skipped'] += 1
                    error_reasons[err.reason] += 1
                if debug:
                    if counter['accumulate'] > 100:
                        break
        logger.info('read all deeds from %s' % path_zip)
        cp_rm = subprocess.run(['rm', path_txt])
        assert cp_rm.returncode == 0
    logger.info('read all deeds zipfiles')
    deed.log_summary()
    for k, v in counter.items():
        logger.info(' %s occured %d times' % (k, v))
    logger.info('reasons taxroll records were skipped')
    for k, v in error_reasons.items():
        logger.info(' %50s: %d times' % (k, v))
    deed.create_table()
    pass


class Neighborhood:
    'singleton class, to group together computation and data around neighborhood features'
    def __init__(self, conn, logger, table_name):
        def code_lusei(description):
            return int(lookup_code(self.conn, self.table_name, 'LUSEI', description))

        def code_propn(description):
            return int(lookup_code(self.conn, self.table_name, 'PROPN', description))

        self.conn = conn
        self.logger = logger
        self.table_name = table_name

        self.propn_skip = set([0])
        self.lusei_skip = set([999])

        # table is ordered by the values of PROPN in the taxroll_codes table
        # Group the descriptions into propn kinds
        propn_kind_description = {
            'Single Family Residence / Townhouse': 'residential',
            'Condominium (residential)': 'residential',
            'Commercial': 'commercial',
            'Duplex, Triplex, Quadplex': 'residential',
            'Apartment': 'residential',
            'Hotel, Motel': 'commercial',
            'Commercial (condominium)': 'commercial',
            'Retail': 'commercial',
            'Service (general public)': 'Service (general public)',
            'Office Building': 'commercial',
            'Warehouse': 'commercial',
            'Financial Institution': 'commercial',
            'Hospital (medical complex, clinic)': 'other',
            'Parking': 'commercial',
            'Amusement-Recreation': 'Amusement-Recreation',
            'Industrial': 'industrial',
            'Industrial Light': 'industrial',
            'Industrial Heavy': 'industrial',
            'Transport': 'other',
            'Utilities': 'other',
            'Agricultural': 'other',
            'Vacant': 'other',
            'Exempt': 'Exempt',
            }

        # convert the descriptions to the codes used in the taxroll records
        self.propn_kinds = {}
        for k, v in propn_kind_description.items():
            self.propn_kinds[code_propn(k)] = v

        lusei_kind_description = {
            'SCHOOL': 'school',
            'NURSERY SCHOOL': 'school',
            'HIGH SCHOOL': 'school',
            'PRIVATE SCHOOL': 'school',
            'VOCATIONAL/TRADE SCHOOL': 'school',
            'SECONDARY EDUCATIONAL SCHOOL': 'school',
            'PUBLIC SCHOOL': 'school',
            'PARK': 'park',
            }

        self.lusei_kinds = {}
        for k, v in lusei_kind_description.items():
            self.lusei_kinds[code_lusei(k)] = v

        self.parcel_count = collections.defaultdict(collections.Counter)
        self.parcel_land_square_footage = collections.defaultdict(collections.Counter)

    def accumulate(self, row) -> bool:
        '''Accumulate lot size of the parcel or raise u.InputError'''
        '''Return True iff neighborhood features were set'''

        def count(census_tract, kind, land_square_footage):
            self.parcel_count[census_tract][kind] += 1
            self.parcel_land_square_footage[census_tract][kind] += land_square_footage

        census_tract = row['CENSUS TRACT']
        propn_code = int(row['PROPERTY INDICATOR CODE'])
        lusei_code = int(row['UNIVERSAL LAND USE CODE'])
        land_square_footage_str = row['LAND SQUARE FOOTAGE']

        if census_tract not in self.parcel_count:
            self.parcel_count[census_tract] = {
                'residential': 0,  # count of parcels that are residential
                'commercial': 0,   # count of parcels that are commercial
                'industrial': 0,   # ...
                'other': 0,
                'total': 0,
                'school': 0,
                'park': 0,
                }

        if census_tract == '':
            raise u.InputError('missing census_tract', census_tract)
        if propn_code in self.propn_skip:
            raise u.InputError('PROPN code is to be skipped', propn_code)
        if lusei_code in self.lusei_skip:
            raise u.InputError('LUSEI code is to be skipped', lusei_code)
        if census_tract == '' or propn_code in self.propn_skip or lusei_code in self.lusei_skip:
            return False
        try:
            land_square_footage = int(land_square_footage_str)
        except ValueError:
            pdb.set_trace()
            raise u.InputError('LAND SQUARE FOOTAGE not an int', land_square_footage_str)

        propn_kind = self.propn_kinds[propn_code]
        lusei_kind = self.lusei_kinds.get(lusei_code, 'not special')
        if propn_kind == 'residential':
            count(census_tract, 'residential', land_square_footage)
        elif propn_kind == 'commercial':
            count(census_tract, 'commercial', land_square_footage)
        elif propn_kind == 'Service (general public)':
            if lusei_kind == 'school':
                count(census_tract, 'school', land_square_footage)
            else:
                count(census_tract, 'other', land_square_footage)
        elif propn_kind == 'industrial':
            count(census_tract, 'industrial', land_square_footage)
        elif propn_kind == 'Amusement-Recreation':
            if lusei_kind == 'park':
                count(census_tract, 'park', land_square_footage)
            else:
                count(census_tract, 'other', land_square_footage)
        elif propn_kind == 'Exempt':
            if lusei_kind == 'school':
                count(census_tract, 'school', land_square_footage)
            else:
                count(census_tract, 'other', land_square_footage)
        elif propn_kind == 'other':
            count(census_tract, 'other', land_square_footage)
        else:
            print('cannot happen', propn_kind, lusei_kind)
            pdb.set_trace()

    def log_summary(self):
        self.logger.info('neighborhood summary')
        parcel_count = self.parcel_count
        parcel_land_square_footage = self.parcel_land_square_footage
        self.logger.info('found %d census tracts' % len(parcel_count))
        for census_tract, census_tract_counts in parcel_count.items():
            # determine totals across kinds
            total_count = 0
            for kind, count in census_tract_counts.items():
                total_count += count
            total_land_square_footage = 0
            for kind, land_square_footage in parcel_land_square_footage[census_tract].items():
                total_land_square_footage += land_square_footage

            # print counts and fractions by kind
            line_counts = 'census_tract %s counts: ' % census_tract
            for kind, count in census_tract_counts.items():
                if count > 0:
                    line_counts += '%s %d ' % (kind, count)
            self.logger.info(line_counts)

            line_land = 'census_tract %s land area: ' % census_tract
            for kind, land_square_footage in parcel_land_square_footage[census_tract].items():
                if land_square_footage > 0:
                    line_land += '%s %4.2f ' % (kind, land_square_footage / total_land_square_footage)
            self.logger.info(line_land)
            self.logger.info('')

    def create_table(self):
        '''insert table into the data base'''
        # create the table
        drop_stmt = '''DROP TABLE IF EXISTS neighborhoods'''
        self.conn.execute(drop_stmt)

        create_stmt = '''CREATE TABLE neighborhoods
        ( census_tract                             text    NOT NULL
        , fraction_land_square_footage_residential real    NOT NULL
        , fraction_land_square_footage_commercial  real    NOT NULL
        , fraction_land_square_footage_industrial  real    NOT NULL
        , fraction_land_square_footage_schools     real    NOT NULL
        , fraction_land_square_footage_parks       real    NOT NULL
        , fraction_land_square_footage_other       real    NOT NULL
        , PRIMARY KEY (census_tract)
        )
        '''
        self.conn.execute(create_stmt)

        # insert each row
        counter = collections.Counter()
        for census_tract, fractions_land_square_footage in self.parcel_land_square_footage.items():
            total = 0.0
            for k, v in fractions_land_square_footage.items():
                total += v
            if total == 0.0:
                # skip census tracts with no land
                print(census_tract, fractions_land_square_footage)
                counter['skipped no land'] += 1
                continue
            self.conn.execute(
                'INSERT INTO neighborhoods VALUES (?, ?, ?, ?, ?, ?, ?)',
                ((census_tract,
                  fractions_land_square_footage.get('residential', 0.0) / total,
                  fractions_land_square_footage.get('commercial', 0.0) / total,
                  fractions_land_square_footage.get('industrial', 0.0) / total,
                  fractions_land_square_footage.get('schools', 0.0) / total,
                  fractions_land_square_footage.get('parks', 0.0) / total,
                  fractions_land_square_footage.get('other', 0.0) / total,
                  )))
            counter['inserted'] += 1
        for k, v in counter.items():
            self.logger.info('neighborhoods %s: %d' % (k, v))


class Parcel:
    def __init__(self, conn, logger):
        self.conn = conn
        self.logger = logger

        self.propn_code_single_family_residential = lookup_code(
            conn,
            'codes_taxrolls',
            'PROPN',
            'Single Family Residence / Townhouse',
        )

        self.features = {}
        self.accumulated = 0

    def accumulate(self, row):
        'accumulate features of the parcel into self.features'''
        debug = False

        def extract_nonnegative_float(field_name):
            try:
                value_str = row[field_name]
                value = float(value_str)
                assert value >= 0.0
                return value
            except Exception:
                raise u.InputError('invalid %s' % field_name, value_str)

        def extract_positive_float(field_name):
            try:
                value_str = row[field_name]
                value = float(value_str)
                assert value > 0.0
                return value
            except Exception:
                raise u.InputError('invalid %s' % field_name, value_str)

        if debug:
            pprint.pprint(row)

        try:
            propn_code = row['PROPERTY INDICATOR CODE']
            assert propn_code == self.propn_code_single_family_residential
        except AssertionError:
            raise u.InputError('not single family residence', propn_code)

        try:
            apn = u.best_apn(row['APN FORMATTED'], row['APN UNFORMATTED'])
        except Exception:
            pdb.set_trace()
            raise u.InputError('invalid APN', (row['APN UNFORMATTED'], row['APN FORMATTED']))

        try:
            census_tract = int(row['CENSUS TRACT'])
        except Exception:
            pdb.set_trace()
            raise u.InputError('invalid census tract', row['CENSUS TRACT'])

        try:
            property_city = row['PROPERTY CITY']
            assert len(property_city) > 0
        except AssertionError:
            raise u.InputError('invalid property_city', row['PROPERTY CITY'])

        total_value_calculated = extract_positive_float('TOTAL VALUE CALCULATED')
        land_square_footage = extract_positive_float('LAND SQUARE FOOTAGE')
        living_square_feet = extract_positive_float('LIVING SQUARE FEET')
        effective_year_built = extract_positive_float('EFFECTIVE YEAR BUILT')
        bedrooms = extract_positive_float('BEDROOMS')
        total_rooms = extract_positive_float('TOTAL ROOMS')
        total_baths = extract_positive_float('TOTAL BATHS')
        fireplace_number = extract_nonnegative_float('FIREPLACE NUMBER')
        parking_spaces = extract_nonnegative_float('PARKING SPACES')
        has_pool = 1.0 if row['POOL FLAG'] == 'Y' else 0.0
        units_number = extract_positive_float('UNITS NUMBER')

        try:
            assert apn not in self.features
        except Exception:
            print('duplicate apn', apn)
            pdb.set_trace()

        self.features[apn] = {
            'census_tract': census_tract,
            'property_city': property_city,
            'total_value_calculated': total_value_calculated,
            'land_square_footage': land_square_footage,
            'living_square_feet': living_square_feet,
            'effective_year_built': effective_year_built,
            'bedrooms': bedrooms,
            'total_rooms': total_rooms,
            'total_baths': total_baths,
            'fireplace_number': fireplace_number,
            'parking_spaces': parking_spaces,
            'has_pool': has_pool,
            'units_number': units_number,
            }
        if debug:
            print('apn', apn)
            pprint.pprint(self.features[apn])
        self.accumulated += 1

    def log_summary(self):
        '''summarize data, including num distinct values, mean, and variance'''
        self.logger.info('parcels summary')
        self.logger.info('created %d SFR parcels' % len(self.features))

        # determine distinct values for each feature
        distinct_values = collections.defaultdict(set)
        for apn, features in self.features.items():
            for feature_name, feature_value in features.items():
                distinct_values[feature_name].add(feature_value)
        for feature_name, distinct_value_list in distinct_values.items():
            self.logger.info('feature %s: %d distinct values' % (feature_name, len(distinct_value_list)))
        self.distinct_values = distinct_values

    def create_tables(self):
        '''create tables into parcels and parcel_feature_statistics'''

        drop_stmt = '''DROP TABLE IF EXISTS parcels'''
        self.conn.execute(drop_stmt)

        create_stmt = '''CREATE TABLE parcels
        ( apn                    integer NOT NULL
        , census_tract           text    NOT NULL
        , property_city          text    NOT NULL
        , total_value_calculated real NOT NULL
        , land_square_footage    real NOT NULL
        , living_square_feet     real NOT NULL
        , effective_year_built   real NOT NULL
        , bedrooms               real NOT NULL
        , total_rooms            real NOT NULL
        , total_baths            real NOT NULL
        , fireplace_number       real NOT NULL
        , parking_spaces         real NOT NULL
        , has_pool               real NOT NULL
        , units_number           real NOT NULL
        , PRIMARY KEY (apn)
        )
        '''
        self.conn.execute(create_stmt)

        # insert each row
        for apn, features in self.features.items():
            self.conn.execute(
                'INSERT INTO parcels VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                ((apn,
                  features['census_tract'],
                  features['property_city'],
                  features['total_value_calculated'],
                  features['land_square_footage'],
                  features['living_square_feet'],
                  features['effective_year_built'],
                  features['bedrooms'],
                  features['total_rooms'],
                  features['total_baths'],
                  features['fireplace_number'],
                  features['parking_spaces'],
                  features['has_pool'],
                  features['units_number'],
                  )))


def read_taxrolls(conn, config, logger):
    '''Create table parcels from data in taxroll zip files'''

    debug = False
    counter = collections.Counter()
    error_reasons = collections.Counter()
    neighborhood = Neighborhood(conn, logger, 'codes_taxrolls')
    parcel = Parcel(conn, logger)
    n_retained = 0
    n_skipped = 0
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

                try:
                    neighborhood.accumulate(row)
                    parcel.accumulate(row)
                    n_retained += 1
                except u.InputError as err:
                    counter['skipped'] += 1
                    error_reasons[err.reason] += 1
                    n_skipped += 1
                    continue
                if debug and parcel.accumulated > 100:
                    break
                continue
        logger.info('read all deeds from %s' % path_zip)
        cp_rm = subprocess.run(['rm', path_txt])
        assert cp_rm.returncode == 0
    print('read all taxroll zipfiles')
    logger.info('retained %d parcels' % n_retained)
    logger.info('skipped %d parcels' % n_skipped)
    logger.info(' ')
    logger.info('reasons parcel was not saved')
    for reason in error_reasons.keys():
        logger.info(' reason %s occured %d times' % (reason, error_reasons[reason]))

    # create neighborhood table
    neighborhood.log_summary()
    neighborhood.create_table()
    parcel.log_summary()
    parcel.create_tables()


class Census:
    def __init__(self, conn, logger):
        self.conn = conn
        self.logger = logger

        self.features = collections.defaultdict(dict)  # key = census_tract  value = map of features
        self.mean_travel_times = {
            'P031003': 2.5,
            'P031004': 7.0,
            'P031005': 12.0,
            'P031006': 17.0,
            'P031007': 22.0,
            'P031008': 27.0,
            'P031009': 32.0,
            'P031010': 37.0,
            'P031011': 42.0,
            'P031012': 47.0,
            'P031013': 72.5,
            'P031014': 110.0,  # 90 minutes or more
        }

    def accumulate(self, row):
        'assumulate features of each census tract'
        try:
            value_str = row['GEO_ID2']
            census_tract = value_str[4:]
            assert len(census_tract) == 6
        except AssertionError:
            pdb.set_trace()
            raise u.InputError('invalid census tract', value_str)

        # mean commute times
        n_in_census_tract = 0
        weighted_sum = 0.0
        for column_name, column_mean_travel_time in self.mean_travel_times.items():
            n_str = row[column_name]
            try:
                n = int(n_str)
            except ValueError:
                raise u.InputError('non-int %s' % column_name, n_str)
            n_in_census_tract += n
            weighted_sum = n * column_mean_travel_time
        if n_in_census_tract == 0:
            raise u.InputError('no commuters in census tract', census_tract)
        mean_commute_time_minutes = weighted_sum / n_in_census_tract

        # median household income
        try:
            value_str = row['P053001']  # in 1999
            median_household_income = float(value_str)
        except ValueError:
            raise u.InputError('non-float median household income', value_str)
        median_household_income = median_household_income

        # fraction of units that are owner occupied
        try:
            total_str = row['H007001']
            total = float(total_str)
        except ValueError:
            raise u.InputError('non-float in total occupied', total_str)

        try:
            owner_str = row['H007002']
            owner = float(owner_str)
        except ValueError:
            raise u.InputError('non-float in owner occupied', owner_str)

        if total == 0.0:
            raise u.InputError('zero residences occupied', None)
        else:
            fraction_owner_occupied = owner / total

        self.features[census_tract]['mean_commute_time_minutes'] = mean_commute_time_minutes
        self.features[census_tract]['median_household_income'] = median_household_income
        self.features[census_tract]['fraction_owner_occupied'] = fraction_owner_occupied

    def log_summary(self):
        pdb.set_trace()
        self.logger.info('found %d census tracts with usable data' % len(self.features))

    def create_table(self):
        stmt_drop = '''DROP TABLE IF EXISTS census'''
        self.conn.execute(stmt_drop)

        stmt_create = '''CREATE TABLE census
        ( census_tract              text NOT NULL
        , mean_commute_time_minutes real NOT NULL
        , median_household_income   real NOT NULL
        , fraction_owner_occupied   real NOT NULL
        , PRIMARY KEY (census_tract)
        )
        '''
        self.conn.execute(stmt_create)

        for census_tract, feature_dict in self.features.items():
            self.conn.execute(
                'INSERT into census VALUES (?, ?, ?, ?)', (
                    census_tract,
                    feature_dict['mean_commute_time_minutes'],
                    feature_dict['median_household_income'],
                    feature_dict['fraction_owner_occupied'],
                    ),
                )


def read_census(conn, config, logger):
    '''Create table census from data in census file'''
    debug = False
    path = os.path.join(config['dir_data'], config['in_census'])
    census = Census(conn, logger)
    n_retained = 0
    n_skipped = 0
    error_reasons = collections.Counter()
    with open(path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t')
        for row_index, row in enumerate(reader):
            if debug:
                print(row_index)
                pprint.pprint(row)
            if row_index == 0:
                continue  # skip explanations of column names
            try:
                census.accumulate(row)
                n_retained += 1
            except u.InputError as err:
                n_skipped += 1
                error_reasons[err.reason] += 1
    logger.info('read all census records')
    logger.info(' retained %d' % n_retained)
    logger.info(' skipped %d' % n_skipped)

    # census.log_summary()
    census.create_table()


def create_transactions(conn, config, logger):
    '''Join deeds, parcels, neighborhoods, and census to create table transactions

    Create feature just_for_experiments in {0, 1} for a random 80% of the transactions.
    '''
    debug = True
    def head(table_name, n=3):
        print('head %s' % table_name)
        i = 0
        stmt = 'SELECT * FROM %s' % table_name
        for row in conn.execute(stmt):
            # OverflowError: signed integer is greater than maximum
            # could be a datetime.date problem
            pprint.pprint(dict(row))
            i += 1
            if i == n:
                break
        
    head('deeds')
    head('parcels')
    head('neighborhoods')
    head('census')
        
    conn.execute('DROP TABLE IF EXISTS transactions')
    # conn.execute(
    #     '''CREATE TABLE transactions AS
    #     SELECT deeds.apn as apn
    #     , deeds.sale_date as sale_date
    #     , parcels.census_tract as census_tract
    #     , neighborhoods.fraction_land_square_footage_residential as census_tract_fraction_land_residential
    #     , census.mean_commute_time_minutes as census_tract_mean_commute_time_minutes
    #     , census.median_household_income as census_tract_median_household_income
    #     , census.fraction_owner_occupied as census_tract_fraction_owner_occupied
    #     FROM deeds
    #     INNER JOIN parcels ON parcels.apn = deeds.apn
    #     INNER JOIN neighborhoods ON neighborhoods.census_tract = parcels.census_tract
    #     INNER JOIN census on census.census_tract = parcels.census_tract
    #     ''')
    # print('first create executed')
    conn.execute(
        '''CREATE TABLE transactions AS
        SELECT deeds.apn as apn
        , deeds.sale_date as sale_date
        , deeds.sale_year as sale_year
        , deeds.sale_month as sale_month
        , deeds.sale_amount as sale_amount
        , parcels.census_tract as census_tract
        , parcels.property_city as property_city
        , parcels.total_value_calculated as total_value_calculated
        , parcels.land_square_footage as land_square_footage
        , parcels.living_square_feet as living_square_feet
        , parcels.effective_year_built as effective_year_built
        , parcels.bedrooms as bedrooms
        , parcels.total_rooms as total_rooms
        , parcels.total_baths as total_baths
        , parcels.fireplace_number as fireplace_number
        , parcels.parking_spaces as parking_spaces
        , parcels.has_pool as has_pool
        , parcels.units_number as units_number
        , neighborhoods.fraction_land_square_footage_residential as census_tract_fraction_land_residential
        , neighborhoods.fraction_land_square_footage_commercial as census_tract_fractin_land_commercial
        , neighborhoods.fraction_land_square_footage_industrial as census_tract_fraction_land_industrial
        , neighborhoods.fraction_land_square_footage_schools as census_tract_fraction_land_schools
        , neighborhoods.fraction_land_square_footage_parks as census_tract_fraction_land_parks
        , census.mean_commute_time_minutes as census_tract_mean_commute_time_minutes
        , census.median_household_income as census_tract_median_household_income
        , census.fraction_owner_occupied as census_tract_fraction_owner_occupied
        FROM deeds
        INNER JOIN parcels ON parcels.apn = deeds.apn
        INNER JOIN neighborhoods ON neighborhoods.census_tract = parcels.census_tract
        INNER JOIN census on census.census_tract = parcels.census_tract
        ''')
    conn.execute(
        '''CREATE INDEX transactions_apn ON transactions (apn)'''
        )
    conn.execute(
        '''CREATE INDEX transactions_sale_date ON transactions (sale_date)'''
        )
    conn.execute(
        '''CREATE INDEX transactions_sale_year ON transactions (sale_year)'''
        )
    conn.execute(
        '''CREATE INDEX transactions_sale_month ON transactions (sale_month)'''
        )
    head('transactions')
    for row in conn.execute('SELECT (SELECT count() FROM transactions) as count, * FROM transactions'):
        print('count transactions = ', row['count'])
        break

    # split the transactions into in_testing and otherwise (in_training)
    # This program stratifies by transaction.sale_date month
    # It marks a random 80% of the transactions in each month as use_for_experiments
    # After the experiments are finished, the best model will be retrained 
    # A prior version did this work in the module samples.py
    # - It used scikit learn cross_validation.StratisfiedShuffleSplit
    conn.execute('ALTER TABLE transactions ADD COLUMN in_training real NOT NULL DEFAULT 0.0')

    def next_year_month():
        '''yield (year, month) for all the prediction periods'''
        date_census_became_known, err1 = u.as_date(config['date_census_became_known'])
        last_date, err1 = u.as_date(config['date_last_transaction'])
        last_year = last_date.year
        last_month = last_date.month
        year = date_census_became_known.year
        month = date_census_became_known.month
        while True:
            yield (year, month)
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            if year == last_year and month > last_month:
                break
        
    random.seed(config['random_seed'])
    all_n_in_training = 0
    all_n_not_in_training = 0
    for year_month in next_year_month():
        year, month = year_month
        select_stmt = 'SELECT * FROM transactions where sale_year = %d and sale_month = %d' % (
            year,
            month,
            )
        n_in_training = 0
        n_not_in_training = 0
        for row in conn.execute(select_stmt):
            r = random.random()  # r in [0.0, 1.0)
            if r < config['fraction_in_training']:
                conn.execute(
                    'UPDATE transactions SET in_training = 1.0 WHERE apn = ? AND sale_date = ?',
                    (row['apn'],
                     row['sale_date']),
                )
                n_in_training += 1
                all_n_in_training += 1
            else:
                conn.execute(
                    'UPDATE transactions SET in_training = 0.0 WHERE apn = ? AND sale_date = ?',
                    (row['apn'],
                     row['sale_date']),
                )
                n_not_in_training += 1
                all_n_not_in_training += 1
        logger.info(
            'transactions for %4d %2d: in_training %5d not in training %5d',
            year,
            month,
            n_in_training,
            n_not_in_training,
            )
    logger.info(
        'transactions for all periods: in_training %d not in training %d',
        all_n_in_training,
        all_n_not_in_training,
        )
    pass


def create_standardize(conn, config, logger):
    '''determine mean and standard deviation of each numeric column in transactions'''
    debug = False
    def summarize(column_name):
        '''return mean, median, standard deviation'''
        stmt_select_column = 'SELECT %s FROM transactions' % column_name
        values = []
        for row in conn.execute(stmt_select_column):
            values.append(row[column_name])
        a = np.array(values)
        return np.mean(a), np.median(a), np.std(a)
    
    conn.execute('DROP TABLE IF EXISTS transactions_standardizers')
    conn.execute(
        '''CREATE TABLE transactions_standardizers
        ( column_name        text NOT NULL
        , mean               real NOT NULL
        , median             real NOT NULL
        , standard_deviation real NOT NULL
        , PRIMARY KEY (column_name)
        )
        '''
        )

    if debug:
        stmt_master = "SELECT * FROM sqlite_master"
        for table_info in conn.execute(stmt_master):
            print(table_info['tbl_name'], table_info['name'], table_info['type'])
    stmt_transactions = "pragma table_info('transactions')"
    for info in conn.execute(stmt_transactions):
        if debug and False:
            column_id, column_name, column_type, column_notnull, column_default, column_pk = info
            print(column_id, column_name, column_type, column_notnull, column_default, column_pk)
        if info['type'] == 'REAL':  # all features have type REAL
            mean, median, std = summarize(info['name'])
            stmt_insert = 'INSERT INTO transactions_standardizers VALUES ("%s", %f, %f, %f)' % (
                info['name'],
                mean,
                median,
                std,
                )
            conn.execute(stmt_insert)
            if debug:
                print(info['name'], mean, median, std)
    pass
        
    
def main(argv):
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
        read_deeds(conn, config, logger)
        read_taxrolls(conn, config, logger)
        read_census(conn, config, logger)
        create_transactions(conn, config, logger)
        create_standardize(conn, config, logger)
    if True:
        pass
    if False:
        delete_intermediate_tables(conn)

    conn.commit()
    conn.close()


if __name__ == '__main__':
    main(sys.argv)
