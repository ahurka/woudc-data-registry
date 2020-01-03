# =================================================================
#
# Terms and Conditions of Use
#
# Unless otherwise noted, computer program source code of this
# distribution # is covered under Crown Copyright, Government of
# Canada, and is distributed under the MIT License.
#
# The Canada wordmark and related graphics associated with this
# distribution are protected under trademark law and copyright law.
# No permission is granted to use them outside the parameters of
# the Government of Canada's corporate identity program. For
# more information, see
# http://www.tbs-sct.gc.ca/fip-pcim/index-eng.asp
#
# Copyright title to all 3rd party software distributed with this
# software is held by the respective copyright holders as noted in
# those files. Users are asked to read the 3rd Party Licenses
# referenced with those assets.
#
# Copyright (c) 2019 Government of Canada
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import csv
import json
import yaml

import re

import jsonschema
import logging

from io import StringIO
from datetime import datetime, time
from collections import OrderedDict

from woudc_data_registry import config
from woudc_data_registry.util import parse_integer_range


LOGGER = logging.getLogger(__name__)

with open(config.WDR_TABLE_SCHEMA) as table_schema_file:
    table_schema = json.load(table_schema_file)
with open(config.WDR_TABLE_CONFIG) as table_definitions:
    DOMAINS = yaml.safe_load(table_definitions)

try:
    jsonschema.validate(DOMAINS, table_schema)
except jsonschema.SchemaError as err:
    LOGGER.critical('Failed to read table definition schema:'
                    ' cannot process incoming files')
    raise err
except jsonschema.ValidationError as err:
    LOGGER.critical('Failed to read table definition file:'
                    ' cannot process incoming files')
    raise err


def non_content_line(line):
    """
    Returns True iff <line> represents a non-content line of an Extended CSV
    file, i.e. a blank line or a comment.

    :param line: List of comma-separated components in an input line.
    :returns: `bool` of whether the line contains no data.
    """

    if len(line) == 0:
        return True
    elif len(line) == 1:
        first = line[0].strip()
        return len(first) == 0 or first.startswith('*')
    else:
        return line[0].strip().startswith('*')


class ExtendedCSV(object):
    """

    minimal WOUDC Extended CSV parser

    https://guide.woudc.org/en/#chapter-3-standard-data-format

    """

    def __init__(self, content, reporter):
        """
        read WOUDC Extended CSV file

        :param content: buffer of Extended CSV data
        :returns: `woudc_data_registry.parser.ExtendedCSV`
        """

        self.extcsv = {}
        self._raw = None

        self._table_count = {}
        self._line_num = {}

        self.warnings = []
        self.errors = []
        self.reports = reporter

        self._noncore_table_schema = None
        self._observations_table = None

        LOGGER.debug('Reading into csv')
        self._raw = content
        reader = csv.reader(StringIO(self._raw))

        LOGGER.debug('Parsing object model')
        parent_table = None
        lines = enumerate(reader, 1)

        success = True
        for line_num, row in lines:
            separators = []
            for bad_sep in ['::', ';', '$', '%', '|', '\\']:
                if not non_content_line(row) and bad_sep in row[0]:
                    separators.append(bad_sep)

            for separator in separators:
                comma_separated = row[0].replace(separator, ',')
                row = next(csv.reader(StringIO(comma_separated)))

                success &= self._add_to_report(16, line_num,
                                               separator=separator)

            if len(row) == 1 and row[0].startswith('#'):  # table name
                parent_table = ''.join(row).lstrip('#').strip()

                try:
                    LOGGER.debug('Found new table {}'.format(parent_table))
                    ln, fields = next(lines)

                    while non_content_line(fields):
                        success &= self._add_to_report(14, ln)
                        ln, fields = next(lines)

                    parent_table = self.init_table(parent_table, fields,
                                                   line_num)
                except StopIteration:
                    success &= self._add_to_report(9, line_num,
                                                   table=parent_table)
            elif len(row) > 0 and row[0].startswith('*'):  # comment
                LOGGER.debug('Found comment')
                continue
            elif non_content_line(row):  # blank line
                LOGGER.debug('Found blank line')
                continue
            elif parent_table is not None and not non_content_line(row):
                table_values = row
                success &= self.add_values_to_table(parent_table, table_values,
                                                    line_num)
            else:
                success &= self._add_to_report(15, line_num, row=','.join(row))

        if not success:
            raise NonStandardDataError(self.errors)

    def _add_to_report(self, error_code, line, **kwargs):
        """
        Submit a warning or error of code <error_code> to the report generator,
        with was found at line <line> in the input file. Uses keyword arguments
        to detail the warning/error message.

        Returns False iff the error is serious enough to abort parsing.
        """

        message, severe = self.reports.add_message(error_code, line, **kwargs)
        if severe:
            LOGGER.error(message)
            self.errors.append(message)
        else:
            LOGGER.warning(message)
            self.warnings.append(message)

        return not severe

    def line_num(self, table):
        """
        Returns the line in the source file at which <table> started.
        If there is no table in the file named <table>, returns None instead.

        :param table: Name of an Extended CSV table.
        :returns: The line number where the table occurs, or None if
                  the table never occurs.
        """

        return self._line_num.get(table, None)

    def table_count(self, table_type):
        """
        Returns the number of tables named <table_type> in the source file.

        :param table_type: Name of an Extended CSV table without suffixes.
        :returns: Number of tables named <table_type> in the input file.
        """

        return self._table_count.get(table_type, 0)

    def init_table(self, table_name, fields, line_num):
        """
        Record an empty Extended CSV table named <table_name> with
        fields given in the list <fields>, which starts at line <line_num>.

        May change the name of the table if a table named <table_name>
        already exists. Returns the table name that ends up being used.

        :param table_name: Name of the new table
        :param fields: List of column names in the new table
        :param line_num: Line number of the table's header (its name)
        :returns: Final name for the new table
        """

        if table_name not in self._table_count:
            self._table_count[table_name] = 1
        else:
            updated_count = self._table_count[table_name] + 1
            self._table_count[table_name] = updated_count
            table_name += '_' + str(updated_count)

        self.extcsv[table_name] = OrderedDict()
        self._line_num[table_name] = line_num

        for field in fields:
            self.extcsv[table_name][field.strip()] = []

        return table_name

    def add_values_to_table(self, table_name, values, line_num):
        """
        Add the raw strings in <values> to the bottom of the columns
        in the tabled named <table_name>.

        Returns whether the operation was successful (no errors occurred).

        :param table_name: Name of the table the values fall under
        :param values: A list of values from one row in the table
        :param line_num: Line number the row occurs at
        :returns: `bool` of whether the operation was successful
        """

        success = True

        fields = self.extcsv[table_name].keys()
        fillins = len(fields) - len(values)

        if fillins < 0:
            success &= self._add_to_report(25, line_num, table=table_name)

        values.extend([''] * fillins)
        values = values[:len(fields)]

        for field, value in zip(fields, values):
            self.extcsv[table_name][field].append(value.strip())

        return success

    def remove_table(self, table_name):
        """
        Remove a table from the memory of this Extended CSV instance.
        Does not alter the source file in any way.

        :param table_name: Name of the table to delete.
        :returns: void
        """

        table_type = table_name.rstrip('0123456789_')

        self.extcsv.pop(table_name)
        self._line_num.pop(table_name)

        if self._table_count[table_type] > 1:
            self._table_count[table_type] -= 1
        else:
            self._table_count.pop(table_type)

    def typecast_value(self, table, field, value, line_num):
        """
        Returns a copy of the string <value> converted to the expected type
        for a column named <field> in table <table>, if possible, or returns
        the original string otherwise.

        :param table: Name of the table where the value was found
        :param field: Name of the column
        :param value: String containing a value
        :param line_num: Line number where the value was found
        :returns: Value cast to the appropriate type for its column
        """

        if value == '':  # Empty CSV cell
            return None

        lowered_field = field.lower()

        try:
            if lowered_field == 'time':
                return self.parse_timestamp(table, value, line_num)
            elif lowered_field == 'date':
                return self.parse_datestamp(table, value, line_num)
            elif lowered_field == 'utcoffset':
                return self.parse_utcoffset(table, value, line_num)
        except Exception as err:
            self._add_to_report(89, line_num, table=table, field=field,
                                reason=err)
            return value

        try:
            if '.' in value:  # Check float conversion
                return float(value)
            elif len(value) > 1 and value.startswith('0'):
                return value
            else:  # Check integer conversion
                return int(value)
        except Exception:  # Default type to string
            return value

    def parse_timestamp(self, table, timestamp, line_num):
        """
        Return a time object representing the time contained in string
        <timestamp> according to the expected HH:mm:SS format with optional
        'am' or 'pm' designation.

        Corrects common formatting errors and performs very simple validation
        checks. Raises ValueError if the string cannot be parsed.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param timestamp: String value taken from a Time column.
        :param line_num: Line number where the value was found.
        :returns: The timestamp converted to a time object.
        """

        success = True

        if timestamp[-2:] in ['am', 'pm']:
            noon_indicator = timestamp[-2:]
            timestamp = timestamp[:-2].strip()
        else:
            noon_indicator = None

        separators = re.findall(r'[^\w\d]', timestamp)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            success &= self._add_to_report(30, line_num, table=table,
                                           separator=separator)

            timestamp = timestamp.replace(separator, ':')

        tokens = timestamp.split(':')
        hour = tokens[0] or '00'
        minute = tokens[1] or '00' if len(tokens) > 1 else '00'
        second = tokens[2] or '00' if len(tokens) > 2 else '00'

        hour_numeric = minute_numeric = second_numeric = None

        try:
            hour_numeric = int(hour)
        except ValueError:
            success &= self._add_to_report(31, line_num, table=table,
                                           component='hour')
        try:
            minute_numeric = int(minute)
        except ValueError:
            success &= self._add_to_report(31, line_num, table=table,
                                           component='minute')
        try:
            second_numeric = int(second)
        except ValueError:
            success &= self._add_to_report(31, line_num, table=table,
                                           component='second')

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Time'
                             .format(table))

        if noon_indicator == 'am' and hour_numeric == 12:
            success &= self._add_to_report(32, line_num, table=table)
            hour_numeric = 0
        elif noon_indicator == 'pm' and hour_numeric not in [12, None]:
            success &= self._add_to_report(32, line_num, table=table)
            hour_numeric += 12

        if second_numeric is not None and second_numeric not in range(0, 60):
            success &= self._add_to_report(33, line_num, table=table,
                                           component='second',
                                           lower='00', upper='59')

            while second_numeric >= 60 and minute_numeric is not None:
                second_numeric -= 60
                minute_numeric += 1
        if minute_numeric is not None and minute_numeric not in range(0, 60):
            success &= self._add_to_report(33, line_num, table=table,
                                           component='minute',
                                           lower='00', upper='59')

            while minute_numeric >= 60 and hour_numeric is not None:
                minute_numeric -= 60
                hour_numeric += 1
        if hour_numeric is not None and hour_numeric not in range(0, 24):
            success &= self._add_to_report(33, line_num, table=table,
                                           component='hour',
                                           lower='00', upper='23')

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Time'
                             .format(table))
        else:
            return time(hour_numeric, minute_numeric, second_numeric)

    def parse_datestamp(self, table, datestamp, line_num):
        """
        Return a date object representing the date contained in string
        <datestamp> according to the expected YYYY-MM-DD format.

        Corrects common formatting errors and performs very simple validation
        checks. Raises ValueError if the string cannot be parsed.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param datestamp: String value taken from a Date column.
        :param line_num: Line number where the value was found.
        :returns: The datestamp converted to a datetime object.
        """

        success = True

        separators = re.findall(r'[^\w\d]', datestamp)
        bad_seps = set(separators) - set('-')

        for separator in bad_seps:
            success &= self._add_to_report(34, line_num, table=table,
                                           separator=separator)

            datestamp = datestamp.replace(separator, '-')

        tokens = datestamp.split('-')

        if len(tokens) == 1:
            success &= self._add_to_report(35, line_num, table=table)
        if len(tokens) < 3:
            success &= self._add_to_report(36, line_num, table=table)
        elif len(tokens) > 3:
            success &= self._add_to_report(37, line_num, table=table)

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Date'
                             .format(table))

        year = month = day = None

        try:
            year = int(tokens[0])
        except ValueError:
            success &= self._add_to_report(38, line_num, table=table,
                                           component='year')
        try:
            month = int(tokens[1])
        except ValueError:
            success &= self._add_to_report(38, line_num, table=table,
                                           component='month')
        try:
            day = int(tokens[2])
        except ValueError:
            success &= self._add_to_report(38, line_num, table=table,
                                           component='day')

        present_year = datetime.now().year
        if year is not None and year not in range(1940, present_year + 1):
            success &= self._add_to_report(39, line_num, table=table,
                                           component='year',
                                           lower='1940', upper='PRESENT')
        if month is not None and month not in range(1, 12 + 1):
            success &= self._add_to_report(39, line_num, table=table,
                                           component='month',
                                           lower='01', upper='12')
        if day is not None and day not in range(1, 31 + 1):
            success &= self._add_to_report(40, line_num, table=table,
                                           lower='01', upper='31')

        if not success:
            raise ValueError('Parsing errors encountered in #{}.Date'
                             .format(table))
        else:
            return datetime.strptime(datestamp, '%Y-%m-%d').date()

    def parse_utcoffset(self, table, utcoffset, line_num):
        """
        Validates the raw string <utcoffset>, converting it to the expected
        format defined by the regular expression (+|-)\\d\\d:\\d\\d:\\d\\d if
        possible. Returns the converted value or else raises a ValueError.

        The other parameters are used for error reporting.

        :param table: Name of table the value was found under.
        :param utcoffset: String value taken from a UTCOffset column.
        :param line_num: Line number where the value was found.
        :returns: The value converted to expected UTCOffset format.
        """

        success = True

        separators = re.findall(r'[^-\+\w\d]', utcoffset)
        bad_seps = set(separators) - set(':')

        for separator in bad_seps:
            success &= self._add_to_report(41, line_num, table=table,
                                           separator=separator)
            utcoffset = utcoffset.replace(separator, ':')

        sign = r'(\+|-|\+-)?'
        delim = r'[^-\+\w\d]'
        mandatory_place = r'([\d]{1,2})'
        optional_place = '(' + delim + r'([\d]{0,2}))?'

        template = '^{sign}{mandatory}{optional}{optional}$' \
                   .format(sign=sign, mandatory=mandatory_place,
                           optional=optional_place)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            sign, hour, _, minute, _, second = match[0]

            if len(hour) < 2:
                success &= self._add_to_report(42, line_num, table=table,
                                               component='hour')
                hour = hour.rjust(2, '0')

            if not minute:
                success &= self._add_to_report(43, line_num, table=table,
                                               component='minute')
                minute = '00'
            elif len(minute) < 2:
                success &= self._add_to_report(42, line_num, table=table,
                                               component='minute')
                minute = minute.rjust(2, '0')

            if not second:
                success &= self._add_to_report(43, line_num, table=table,
                                               component='second')
                second = '00'
            elif len(second) < 2:
                success &= self._add_to_report(42, line_num, table=table,
                                               component='second')
                second = second.rjust(2, '0')

            if all([hour == '00', minute == '00', second == '00']):
                if sign != '+':
                    success &= self._add_to_report(45, line_num, table=table,
                                                   sign='+')
                    sign = '+'
            elif not sign:
                success &= self._add_to_report(44, line_num, table=table)
                sign = '+'
            elif sign == '+-':
                success &= self._add_to_report(45, line_num, table=table,
                                               sign='-')
                sign = '-'

            if not success:
                raise ValueError('Parsing errors encountered in #{}.UTCOffset'
                                 .format(table))
            try:
                magnitude = time(int(hour), int(minute), int(second))
                return '{}{}'.format(sign, magnitude)
            except (ValueError, TypeError) as err:
                self._add_to_report(47, line_num, table=table)
                raise err

        template = '^{sign}[0]+{delim}?[0]*{delim}?[0]*$' \
                   .format(sign=sign, delim=delim)
        match = re.findall(template, utcoffset)

        if len(match) == 1:
            if self._add_to_report(46, line_num, table=table):
                return '+00:00:00'
            else:
                raise ValueError('Parsing errors encountered in #{}.UTCOffset'
                                 .format(table))

        self._add_to_report(47, line_num, table=table)
        raise ValueError('Parsing errors encountered in #{}.UTCOffset'
                         .format(table))

    def gen_woudc_filename(self):
        """generate WOUDC filename convention"""

        timestamp = self.extcsv['TIMESTAMP']['Date'].strftime('%Y%m%d')
        instrument_name = self.extcsv['INSTRUMENT']['Name']
        instrument_model = self.extcsv['INSTRUMENT']['Model']

        extcsv_serial = self.extcsv['INSTRUMENT'].get('Number', None)
        instrument_number = extcsv_serial or 'na'

        agency = self.extcsv['DATA_GENERATION']['Agency']

        filename = '{}.{}.{}.{}.{}.csv'.format(timestamp, instrument_name,
                                               instrument_model,
                                               instrument_number, agency)
        if ' ' in filename:
            LOGGER.warning('filename contains spaces: {}'.format(filename))
            file_slug = filename.replace(' ', '-')
            LOGGER.info('filename {} renamed to {}'
                        .format(filename, file_slug))
            filename = file_slug

        return filename

    def collimate_tables(self, tables, schema):
        """
        Convert the lists of raw strings in all the tables in <tables>
        into processed values of an appropriate type.

        Ensure that all one-row tables have their single value reported
        for each field rather than a list containing the one value.

        Assumes that all tables in <tables> are demonstratedly valid.

        :param tables: List of tables in which to process columns.
        :param schema: A series of table definitions for the input file.
        :returns: void
        """

        for table_name in tables:
            table_type = table_name.rstrip('0123456789_')
            body = self.extcsv[table_name]

            table_valueline = self.line_num(table_name) + 2

            for field, column in body.items():
                converted = [
                    self.typecast_value(table_name, field, val, line)
                    for line, val in enumerate(column, table_valueline)
                ]

                if schema[table_type]['rows'] == 1:
                    if len(converted) > 0:
                        self.extcsv[table_name][field] = converted[0]
                    else:
                        self.extcsv[table_name][field] = ''
                else:
                    self.extcsv[table_name][field] = converted

    def check_table_occurrences(self, schema):
        """
        Validate the number of occurrences of each table type in <schema>
        against the expected range of occurrences for that type.
        Returns True iff all tables occur an acceptable number of times.

        :param schema: A series of table definitions for the input file.
        :returns: `bool` of whether all tables are within the expected
                  occurrence range.
        """

        success = True

        for table_type in schema.keys():
            count = self.table_count(table_type)

            occurrence_range = str(schema[table_type]['occurrences'])
            lower, upper = parse_integer_range(occurrence_range)

            if count < lower:
                line = self.line_num(table_type + '_' + str(count))
                success &= self._add_to_report(26, line, table=table_type,
                                               bound=lower)
            if count > upper:
                line = self.line_num(table_type + '_' + str(upper + 1))
                success &= self._add_to_report(27, line, table=table_type,
                                               bound=upper)

        return success

    def check_table_height(self, table, definition, num_rows):
        """
        Validate the number of rows in the table named <table> against the
        expected range of rows assuming the table has <num_rows> rows.
        Returns True iff the table has an acceptable number of rows.

        :param table: Name of a table in the input file.
        :param definition: Schema definition of <table>.
        :param num_rows: The number of rows in <table>.
        :returns: `bool` of whether all tables are in the expected
                  height range.
        """

        height_range = str(definition['rows'])
        lower, upper = parse_integer_range(height_range)

        occurrence_range = str(definition['occurrences'])
        is_required, _ = parse_integer_range(occurrence_range)

        headerline = self.line_num(table)
        success = True

        if num_rows == 0:
            if is_required:
                success &= self._add_to_report(11, headerline, table=table)
            else:
                success &= self._add_to_report(12, headerline, table=table)
        elif num_rows < lower:
            success &= self._add_to_report(28, headerline, table=table,
                                           bound=lower)
        elif num_rows > upper:
            success &= self._add_to_report(29, headerline, table=table,
                                           bound=upper)

        return success

    def check_field_validity(self, table, definition):
        """
        Validates the fields of the table named <table>, ensuring that
        all required fields are present, correcting capitalizations,
        creating empty columns for any optional fields that were not
        provided, and removing any unrecognized fields.

        Returns True if the table's fields satisfy its definition.

        :param table: Name of a table in the input file.
        :param definition: Schema definition for the table.
        :returns: `bool` of whether fields satisfy the table's definition.
        """

        success = True

        required = definition.get('required_fields', ())
        optional = definition.get('optional_fields', ())
        provided = self.extcsv[table].keys()

        required_case_map = {key.lower(): key for key in required}
        optional_case_map = {key.lower(): key for key in optional}
        provided_case_map = {key.lower(): key for key in provided}

        missing_fields = [field for field in required
                          if field not in provided]
        extra_fields = [field for field in provided
                        if field.lower() not in required_case_map]

        fieldline = self.line_num(table) + 1
        valueline = fieldline + 1

        arbitrary_column = next(iter(self.extcsv[table].values()))
        num_rows = len(arbitrary_column)
        null_value = [''] * num_rows

        # Attempt to find a match for all required missing fields.
        for missing in missing_fields:
            match_insensitive = provided_case_map.get(missing.lower(), None)
            if match_insensitive:
                success &= self._add_to_report(20, fieldline, table=table,
                                               oldfield=match_insensitive,
                                               newfield=missing)

                self.extcsv[table][missing] = \
                    self.extcsv[table].pop(match_insensitive)
            else:
                success &= self._add_to_report(5, fieldline, table=table,
                                               field=missing)
                self.extcsv[table][missing] = null_value

        if len(missing_fields) == 0:
            LOGGER.debug('No missing fields in table {}'.format(table))
        else:
            LOGGER.info('Filled missing fields with null string values')

        # Assess whether non-required fields are optional fields or
        # excess ones that are not part of the table's schema.
        for extra in extra_fields:
            match_insensitive = optional_case_map.get(extra.lower(), None)

            if match_insensitive:
                LOGGER.info('Found optional field #{}.{}'
                            .format(table, extra))

                if extra != match_insensitive:
                    success &= self._add_to_report(20, fieldline, table=table,
                                                   oldfield=extra,
                                                   newfield=match_insensitive)

                    self.extcsv[table][match_insensitive] = \
                        self.extcsv[table].pop(extra)
            else:
                success &= self._add_to_report(6, fieldline, table=table,
                                               field=extra)
                del self.extcsv[table][extra]

        # Check that no required fields have empty values.
        for field in required:
            column = self.extcsv[table][field]

            if '' in column:
                line = valueline + column.index('')
                success &= self._add_to_report(7, line, table=table,
                                               field=field)

        return success

    def number_of_observations(self):
        """
        Returns the total number of unique rows in the Extended CSV's
        data table(s).

        :returns: Number of unique data rows in the file.
        """

        if not self._observations_table:
            try:
                self._determine_noncore_schema()
            except (NonStandardDataError, MetadataValidationError) as err:
                LOGGER.warning('Cannot identify data table due to: {}'
                               .format(err))
                return 0

        # Count lines in the file's data table(s)
        data_rows = set()
        for i in range(1, self.table_count(self._observations_table) + 1):
            table_name = self._observations_table + '_' + str(i) \
                if i > 1 else self._observations_table

            rows = zip(*self.extcsv[table_name].values())
            data_rows.update(rows)

        return len(data_rows)

    def validate_metadata_tables(self):
        """validate core metadata tables and fields"""

        schema = DOMAINS['Common']
        success = True

        missing_tables = [table for table in schema.keys()
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]

        if len(present_tables) == 0:
            if not self._add_to_report(2, None):
                raise NonStandardDataError(self.errors)
        elif len(missing_tables) > 0:
            for missing in missing_tables:
                success &= self._add_to_report(3, None, table=missing)
        else:
            LOGGER.debug('No missing metadata tables')

        success &= self.check_table_occurrences(schema)

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            definition = schema[table_type]
            body = self.extcsv[table]

            arbitrary_column = next(iter(body.values()))
            num_rows = len(arbitrary_column)

            success &= self.check_field_validity(table, definition)
            success &= self.check_table_height(table, definition, num_rows)

            for field in definition.get('optional_fields', []):
                if field not in self.extcsv[table]:
                    self.extcsv[table][field] = [''] * num_rows

        self.collimate_tables(present_tables, schema)

        if success:
            LOGGER.debug('All core tables in file validated.')
        else:
            raise MetadataValidationError('Invalid metadata', self.errors)

    def _determine_noncore_schema(self):
        """
        Sets the table definitions schema and observations data table
        for this Extended CSV instance, based on its CONTENT fields
        and which tables are present.

        :returns: void
        """

        tables = DOMAINS['Datasets']
        curr_dict = tables
        fieldline = self.line_num('CONTENT') + 1

        for field in [('Category', str), ('Level', float), ('Form', int)]:
            field_name, type_converter = field
            key = str(type_converter(self.extcsv['CONTENT'][field_name]))

            if key in curr_dict:
                curr_dict = curr_dict[key]
            else:
                field_name = '#CONTENT.{}'.format(field_name.capitalize())

                self._add_to_report(61, fieldline, field=field_name)
                return False

        if '1' in curr_dict.keys():
            version = self._determine_version(curr_dict)
            LOGGER.info('Identified version as {}'.format(version))

            curr_dict = curr_dict[version]

        self._noncore_table_schema = {k: v for k, v in curr_dict.items()}
        self._observations_table = self._noncore_table_schema.pop('data_table')

    def _determine_version(self, schema):
        """
        Attempt to determine which of multiple possible table definitions
        contained in <schema> fits the instance's Extended CSV file best,
        based on which required or optional tables are present.

        Returns the best-fitting version, or raises an error if there is
        not enough information.

        :param schema: Dictionary with nested dictionaries of
                       table definitions as values.
        :returns: Version number for the best-fitting table definition.
        """

        versions = set(schema.keys())
        tables = {version: schema[version].keys() for version in versions}
        uniques = {}

        for version in versions:
            u = set(tables[version])
            others = versions - {version}

            for other_version in others:
                u -= set(tables[other_version])

            uniques[version] = u

        candidates = {version: [] for version in versions}
        for table in self.extcsv:
            for version in versions:
                if table in uniques[version]:
                    candidates[version].append(table)

        def rating(version):
            return len(candidates[version]) / len(uniques[version])

        candidate_scores = list(map(rating, versions))
        best_match = max(candidate_scores)
        if best_match == 0:
            self._add_to_report(13, None)
            raise NonStandardDataError(self.errors)
        else:
            for version in versions:
                if rating(version) == best_match:
                    return version

    def validate_dataset_tables(self):
        """Validate tables and fields beyond the core metadata tables"""

        if not self._noncore_table_schema:
            self._determine_noncore_schema()

        schema = self._noncore_table_schema
        success = True

        required_tables = [name for name, body in schema.items()
                           if 'required_fields' in body]
        optional_tables = [name for name, body in schema.items()
                           if 'required_fields' not in body]

        missing_tables = [table for table in required_tables
                          if table not in self.extcsv.keys()]
        present_tables = [table for table in self.extcsv.keys()
                          if table.rstrip('0123456789_') in schema]

        required_tables.extend(DOMAINS['Common'].keys())
        extra_tables = [table for table in self.extcsv.keys()
                        if table.rstrip('0123456789_') not in required_tables]

        dataset = self.extcsv['CONTENT']['Category']
        for missing in missing_tables:
            success &= self._add_to_report(3, None, table=missing)

        success &= self.check_table_occurrences(schema)

        for table in present_tables:
            table_type = table.rstrip('0123456789_')
            definition = schema[table_type]
            body = self.extcsv[table]

            arbitrary_column = next(iter(body.values()))
            num_rows = len(arbitrary_column)

            success &= self.check_field_validity(table, definition)
            success &= self.check_table_height(table, definition, num_rows)

            LOGGER.debug('Finished validating table {}'.format(table))

        for table in extra_tables:
            table_type = table.rstrip('0123456789_')
            if table_type not in optional_tables:
                success &= self._add_to_report(4, None, table=table,
                                               dataset=dataset)
                del self.extcsv[table]

        for table in optional_tables:
            if table not in self.extcsv:
                LOGGER.warning('Optional table {} is not in file.'.format(
                               table))

        for i in range(1, self.table_count(observations_table) + 1):
            table_name = observations_table + '_' + str(i) \
                if i > 1 else observations_table
            arbitrary_column = next(iter(self.extcsv[table_name].values()))

            self.number_of_observations += len(arbitrary_column)

        self.collimate_tables(present_tables, schema)
        schema['data_table'] = observations_table

        if success:
            return success
        else:
            raise MetadataValidationError('Invalid dataset tables',
                                          self.errors)


class NonStandardDataError(Exception):
    """custom exception handler"""

    def __init__(self, errors):
        error_string = '\n' + '\n'.join(map(str, errors))
        super(NonStandardDataError, self).__init__(error_string)

        self.errors = errors


class MetadataValidationError(Exception):
    """custom exception handler"""

    def __init__(self, message, errors):
        """set error list/stack"""
        super(MetadataValidationError, self).__init__(message)
        self.errors = errors
