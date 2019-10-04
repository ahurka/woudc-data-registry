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


import os
import re

import shutil
import logging

from datetime import datetime, time

from woudc_data_registry import config, registry, search
from woudc_data_registry.models import (Contributor, DataRecord, Dataset,
                                        Deployment, Instrument, Project,
                                        Station)
from woudc_data_registry.parser import (DOMAINS, ExtendedCSV,
                                        MetadataValidationError,
                                        NonStandardDataError)
from woudc_data_registry.util import is_text_file, read_file

LOGGER = logging.getLogger(__name__)


class Process(object):
    """
    Generic processing definition

    - detect
    - parse
    - validate
    - verify
    - register
    - index
    """

    def __init__(self):
        """constructor"""

        self.status = None
        self.code = None
        self.message = None
        self.process_start = datetime.utcnow()
        self.process_end = None
        self.registry = registry.Registry()

    def process_data(self, infile, verify_only=False, bypass=False):
        """
        process incoming data record

        :param infile: incoming filepath
        :param verify_only: perform verification only (no ingest)
        :param bypass: skip permission prompts

        :returns: `bool` of processing result
        """

        # detect incoming data file
        data = None
        self.extcsv = None
        self.data_record = None
        self.search_engine = search.SearchIndex()

        self.warnings = []
        self.errors = []

        LOGGER.info('Processing file {}'.format(infile))
        LOGGER.info('Detecting file')
        if not is_text_file(infile):
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = 'binary file detected'
            LOGGER.error('Unknown file: {}'.format(self.message))
            return False

        try:
            data = read_file(infile)
        except UnicodeDecodeError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Unknown file: {}'.format(err))
            return False

        try:
            LOGGER.info('Parsing data record')
            self.extcsv = ExtendedCSV(data)
            LOGGER.info('Validating Extended CSV')
            self.extcsv.validate_metadata()
            LOGGER.info('Valid Extended CSV')
        except NonStandardDataError as err:
            self.status = 'failed'
            self.code = 'NonStandardDataError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err))
            return False
        except MetadataValidationError as err:
            self.status = 'failed'
            self.code = 'MetadataValidationError'
            self.message = err
            LOGGER.error('Invalid Extended CSV: {}'.format(err.errors))
            return False

        LOGGER.info('Data is valid Extended CSV')

#        domains_to_check = [
#            'content_category',
#            'data_generation_agency',
#            'platform_type',
#            'platform_id',
#            'platform_name',
#            'platform_country',
#            'instrument_name',
#            'instrument_model'
#        ]

#        for domain_to_check in domains_to_check:
#            value = getattr(self.data_record, domain_to_check)
#            domain = getattr(DataRecord, domain_to_check)
#
#            if value not in self.registry.query_distinct(domain):
#                msg = 'value {} not in domain {}'.format(value,
#                                                         domain_to_check)
#                LOGGER.error(msg)
#                # raise ProcessingError(msg)

        LOGGER.info('Verifying data record against core metadata fields')

        project_ok = self.check_project()
        dataset_ok = self.check_dataset()

        contributor_ok = self.check_contributor()
        platform_ok = self.check_station()

        LOGGER.debug('Validating agency deployment')
        deployment_ok = self.check_deployment()

        if not deployment_ok:
            deployment_id = ':'.join([platform_id, agency, project])
            deployment_name = '{}@{}'.format(agency, platform_id)
            LOGGER.warning('Deployment {} not found'.format(deployment_id))

            if bypass:
                LOGGER.info('Bypass mode. Skipping permission check')
                permission = True
            else:
                response = input('Deployment {} not found. Add? [y/n] '
                                 .format(deployment_name))
                permission = response.lower() in ['y', 'yes']

            if permission:
                self.add_deployment()
                deployment_ok = True

                msg = 'New deployment {} added'.format(deployment_name)
                self.warnings.append((202, msg, None))
            else:
                msg = 'Deployment {} not added. Skipping file.' \
                      .format(deployment_id)
                LOGGER.error(msg)

                msg = 'No deployment {} found in registry' \
                       .format(deployment_id)
                line = self.extcsv.line_num['PLATFORM'] + 2
                self.errors.append((65, msg, line))

        LOGGER.debug('Validating instrument')
        instrument_ok = self.check_instrument()

        if not instrument_ok:
            old_serial = str(self.extcsv.extcsv['INSTRUMENT']['Number'])
            new_serial = old_serial.lstrip('0')
            LOGGER.debug('Attempting to search instrument serial number {}'
                         .format(new_serial))

            self.extcsv.extcsv['INSTRUMENT']['Number'] = new_serial
            instrument_ok = self.check_instrument()

        if not instrument_ok:
            LOGGER.warning('No instrument with serial {} found in registry'
                           .format(old_serial))
            self.extcsv.extcsv['INSTRUMENT']['Number'] = old_serial
            instrument_ok = self.add_instrument(verify_only)

            if instrument_ok:
                msg = 'New instrument serial number added'
                self.warnings.append((201, msg, None))
            else:
                msg = 'Failed to validate instrument against registry'
                line = self.extcsv.line_num['INSTRUMENT'] + 2
                self.errors.append((139, msg, line))

        instrument_args = [self.extcsv.extcsv['INSTRUMENT']['Name'],
            self.extcsv.extcsv['INSTRUMENT']['Model'],
            str(self.extcsv.extcsv['INSTRUMENT']['Number']),
            str(self.extcsv.extcsv['PLATFORM']['ID']),
            self.extcsv.extcsv['CONTENT']['Category']]
        instrument_id = ':'.join(instrument_args) if instrument_ok else None
        location_ok = self.check_location(instrument_id)

        content_ok = self.check_content_consistency()
        data_generation_ok = self.check_data_generation_consistency()

        if not all([project_ok, dataset_ok, contributor_ok,
                    platform_ok, deployment_ok, instrument_ok,
                    location_ok, content_ok, data_generation_ok]):
            return False

        LOGGER.info('Validating data record')
        self.data_record = DataRecord(self.extcsv)
        self.data_record.ingest_filepath = infile
        self.data_record.filename = os.path.basename(infile)
        self.data_record.url = self.data_record.get_waf_path(
            config.WDR_WAF_BASEURL)
        self.process_end = datetime.utcnow()

        LOGGER.debug('Verifying if URN already exists')
        results = self.registry.query_by_field(
            DataRecord, 'identifier', self.data_record.identifier)

        if results:
            msg = 'Data exists'
            self.status = 'failed'
            self.code = 'ProcessingError'
            self.message = msg
            LOGGER.error(msg)
            return False

        LOGGER.info('Data record is valid and verified')

        if verify_only:  # do not save or index
            LOGGER.info('Verification mode detected. NOT saving to registry')
            return True

        LOGGER.info('Saving data record CSV to registry')
        self.registry.save(self.data_record)

        LOGGER.info('Saving data record CSV to WAF')
        waf_filepath = self.data_record.get_waf_path(config.WDR_WAF_BASEDIR)
        os.makedirs(os.path.dirname(waf_filepath), exist_ok=True)
        shutil.copy2(self.data_record.ingest_filepath, waf_filepath)

        LOGGER.info('Indexing data record search engine')
        version = self.search_engine.get_record_version(self.data_record.es_id)
        if version:
            if version < self.data_record.data_generation_version:
                self.search_engine.index_data_record(
                    self.data_record.__geo_interface__)
        else:
            self.search_engine.index_data_record(
                    self.data_record.__geo_interface__)
        return True

    def add_deployment(self):
        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']
        date = self.extcsv.extcsv['TIMESTAMP']['Date']

        deployment_id = ':'.join([station, agency, project])
        deployment_model = {
            'identifier': deployment_id,
            'station_id': station,
            'contributor_id': agency,
            'start_date': date,
            'end_date': date
        }

        deployment = Deployment(deployment_model)
        self.registry.save(deployment)

    def add_instrument(self, verify_only):
        name = self.extcsv.extcsv['INSTRUMENT']['Name']
        model = str(self.extcsv.extcsv['INSTRUMENT']['Model'])
        serial = str(self.extcsv.extcsv['INSTRUMENT']['Number'])
        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        dataset = self.extcsv.extcsv['CONTENT']['Category']
        location = [self.extcsv.extcsv['LOCATION'].get(f, None)
                    for f in ['Longitude', 'Latitude', 'Height']]

        instrument_id = ':'.join([name, model, serial, station, dataset])
        model = {
            'identifier': instrument_id,
            'name': name,
            'model': model,
            'serial': serial,
            'station_id': station,
            'dataset_id': dataset,
            'x': location[0],
            'y': location[1],
            'z': location[2]
        }

        fields = ['name', 'model', 'station_id', 'dataset_id']
        case_insensitive = ['name', 'model']
        result = self.registry.query_multiple_fields(Instrument, model,
                                                     fields, case_insensitive)
        if result:
            model['name'] = result.name
            model['model'] = result.model
            self.extcsv.extcsv['INSTRUMENT']['Name'] = result.name
            self.extcsv.extcsv['INSTRUMENT']['Model'] = result.model

            LOGGER.debug('All other instrument data matches.')
            LOGGER.info('Adding instrument with new serial number...')

            if verify_only:
                msg = 'Verification mode detected. Instrument not added.'
                LOGGER.info(msg)
            else:
                instrument = Instrument(model)
                self.registry.save(instrument)
                LOGGER.info('Instrument successfully added.')
            return True
        else:
            return False

    def check_project(self):
        project = self.extcsv.extcsv['CONTENT']['Class']

        LOGGER.debug('Validating project {}'.format(project))
        self.projects = self.registry.query_distinct(Project.identifier)

        if not project:
            self.extcsv.extcsv['CONTENT']['Class'] = project = 'WOUDC'
            msg = 'Missing #CONTENT.Class: default to "WOUDC"'
            line = self.extcsv.line_num['CONTENT'] + 2
            self.warnings.append((52, msg, line))

        if project in self.projects:
            LOGGER.debug('Match found for project {}'.format(project))
            return True
        else:
            msg = 'Project {} not found in registry'.format(project)
            line = self.extcsv.line_num['CONTENT'] + 2

            self.errors.append((53, msg, line))
            return False

    def check_dataset(self):
        dataset = self.extcsv.extcsv['CONTENT']['Category']

        LOGGER.debug('Validating dataset {}'.format(dataset))
        self.datasets = self.registry.query_distinct(Dataset.identifier)

        if dataset in self.datasets:
            LOGGER.debug('Match found for dataset {}'.format(dataset))
            return True
        else:
            msg = 'Dataset {} not found in registry'.format(dataset)
            line = self.extcsv.line_num['CONTENT'] + 2

            self.errors.append((56, msg, line))
            return False

    def check_contributor(self):
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']

        LOGGER.debug('Validating contributor {} under project {}'
                     .format(agency, project))
        contributor = {
            'identifier': '{}:{}'.format(agency, project),
            'project_id': project
        }

        fields = ['identifier']
        result = self.registry.query_multiple_fields(Contributor, contributor,
                                                     fields, fields)
        if result:
            contributor_name = result.identifier.split(':')[0]
            self.extcsv.extcsv['DATA_GENERATION']['Agency'] = contributor_name

            LOGGER.debug('Match found for contributor ID {}'
                         .format(result.identifier))
            return True
        else:
            msg = 'Contributor {} not found in registry' \
                  .format(contributor['identifier'])
            line = self.extcsv.line_num['DATA_GENERATION'] + 2

            self.errors.append((127, msg, line))
            return False

    def check_station(self):
        identifier = str(self.extcsv.extcsv['PLATFORM']['ID'])
        pl_type = self.extcsv.extcsv['PLATFORM']['Type']
        name = self.extcsv.extcsv['PLATFORM']['Name']
        country = self.extcsv.extcsv['PLATFORM']['Country']
        gaw_id = str(self.extcsv.extcsv['PLATFORM'].get('GAW_ID', '')) or None

        # TODO: consider adding and checking #PLATFORM_Type
        LOGGER.debug('Validating station {}:{}'.format(identifier, name))

        if pl_type == 'SHP' and any([not country, country == '*IW']):
            self.extcsv.extcsv['PLATFORM']['Country'] = country = 'XY'

            msg = 'Ship #PLATFORM.Country = *IW corrected to Country = XY' \
                  ' to meet ISO-3166 standards'
            line = self.extcsv.line_num['PLATFORM'] + 2
            self.warnings.append((105, msg, line))

        station = {
            'identifier': identifier,
            'type': pl_type,
            'name': name,
            'country_id': country
        }

        LOGGER.debug('Validating station id...')
        values_line = self.extcsv.line_num['PLATFORM'] + 2
        result = self.registry.query_by_field(Station, 'identifier',
                                              identifier)
        if result:
            LOGGER.debug('Validated station with id: {}'.format(identifier))
        else:
            msg = 'Station {} not found in registry'.format(identifier)
            self.errors.append((129, msg, values_line))
            return False

        LOGGER.debug('Validating station type...')
        platform_types = ['STN', 'SHP']
        type_ok = pl_type in platform_types

        if type_ok:
            LOGGER.debug('Validated station type {}'.format(type_ok))
        else:
            msg = 'Station type {} not found in registry'.format(pl_type)
            self.errors.append((128, msg, values_line))

        LOGGER.debug('Validating station name...')
        fields = ['identifier', 'name']
        result = self.registry.query_multiple_fields(Station, station,
                                                     fields, ['name'])
        name_ok = bool(result)
        if name_ok:
            self.extcsv.extcsv['PLATFORM']['Name'] = name = result.name
            LOGGER.debug('Validated with name {} for id {}'.format(
                name, identifier))
        else:
            msg = 'Station name: {} did not match data for id: {}' \
                  .format(name, identifier)
            self.errors.append((130, msg, values_line))

        LOGGER.debug('Validating station country...')
        fields = ['identifier', 'country_id']
        result = self.registry.query_multiple_fields(Station, station,
                                                     fields, ['country_id'])
        country_ok = bool(result)
        if country_ok:
            country = result.country
            self.extcsv.extcsv['PLATFORM']['Country'] = country
            LOGGER.debug('Validated with country: {} for id: {}'
                         .format(country, identifier))
        else:
            msg = 'Station country: {} did not match data for id: {}' \
                  .format(country, identifier)
            self.errors.append((131, msg, values_line))

        return type_ok and name_ok and country_ok

    def check_deployment(self):
        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        agency = self.extcsv.extcsv['DATA_GENERATION']['Agency']
        project = self.extcsv.extcsv['CONTENT']['Class']
        date = self.extcsv.extcsv['TIMESTAMP']['Date']

        deployment_id = ':'.join([station, agency, project])
        results = self.registry.query_by_field(Deployment, 'identifier',
                                               deployment_id)
        if not results:
            LOGGER.warning('Deployment {} not found'.format(deployment_id))
            return False
        else:
            deployment = results[0]
            LOGGER.debug('Found deployment match for {}'
                         .format(deployment_id))
            if deployment.start_date > date:
                deployment.start_date = date
                self.registry.save()
                LOGGER.debug('Deployment start date updated.')
            elif deployment.end_date < date:
                deployment.end_date = date
                self.registry.save()
                LOGGER.debug('Deployment end date updated.')
            return True

    def check_instrument(self):
        name = self.extcsv.extcsv['INSTRUMENT']['Name']
        model = str(self.extcsv.extcsv['INSTRUMENT']['Model'])
        serial = str(self.extcsv.extcsv['INSTRUMENT']['Number'])
        station = str(self.extcsv.extcsv['PLATFORM']['ID'])
        dataset = self.extcsv.extcsv['CONTENT']['Category']

        if not name or name.lower() in ['na', 'n/a']:
            self.extcsv.extcsv['INSTRUMENT']['Name'] = name = 'UNKNOWN'
        if not model or model.lower() in ['na', 'n/a']:
            self.extcsv.extcsv['INSTRUMENT']['Model'] = model = 'UNKNOWN'

        values_line = self.extcsv.line_num['INSTRUMENT'] + 2
        if name == 'UNKNOWN':
            msg = '#INSTRUMENT.Name must not be null'
            self.errors.append((72, msg, values_line))
            return False

        instrument_id = ':'.join([name, model, serial, station, dataset])
        instrument = {
            'name': name,
            'model': model,
            'serial': serial,
            'station_id': station,
            'dataset_id': dataset
        }

        fields = list(instrument.keys())
        case_insensitive = ['name', 'model', 'serial']
        result = self.registry.query_multiple_fields(Instrument, instrument,
                                                     fields, case_insensitive)
        if not result:
            LOGGER.warning('No instrument {} found in registry'
                           .format(instrument_id))
            return False
        else:
            LOGGER.debug('Found instrument match for {}'
                         .format(instrument_id))

            self.extcsv.extcsv['INSTRUMENT']['Name'] = result.name
            self.extcsv.extcsv['INSTRUMENT']['Model'] = result.model
            self.extcsv.extcsv['INSTRUMENT']['Number'] = result.serial
            return True

    def check_location(self, instrument_id):
        lat = self.extcsv.extcsv['LOCATION']['Latitude']
        lon = self.extcsv.extcsv['LOCATION']['Longitude']
        height = self.extcsv.extcsv['LOCATION'].get('Height', None)
        values_line = self.extcsv.line_num['LOCATION'] + 2

        try:
            lat_numeric = float(lat)
            if -90 <= lat_numeric <= 90:
                LOGGER.debug('Validated instrument latitude')
            else:
                msg = '#LOCATION.Latitude is not within the range [-90]-[90]'
                self.warnings.append((76, msg, values_line))
            lat_ok = True
        except ValueError:
            msg = '#LOCATION.Latitude contains invalid characters'
            LOGGER.error(msg)
            self.errors.append((75, msg, values_line))
            lat_numeric = None
            lat_ok = False

        try:
            lon_numeric = float(lon)
            if -180 <= lon_numeric <= 180:
               LOGGER.debug('Validated instrument longitude')
            else:
                msg = '#LOCATION.Longitude is not within the range [-180]-[180]'
                self.warnings.append((76, msg, values_line))
            lon_ok = True
        except ValueError:
            msg = '#LOCATION.Longitude contains invalid characters'
            LOGGER.error(msg)
            self.errors.append((75, msg, values_line))
            lon_numeric = None
            lon_ok = False

        try:
            height_numeric = float(height) if height else None
            if not height or -50 <= height_numeric <= 5100:
                LOGGER.debug('Validated instrument height')
            else:
                msg = '#LOCATION.Height is not within the range [-50]-[5100]'
                self.warnings.append((76, msg, values_line))
            height_ok = True
        except ValueError:
            msg = '#LOCATION.Height contains invalid characters'
            self.warning.append((75, msg, values_line))
            height_numeric = None
            height_ok = False

        if instrument_id is not None:
            result = self.registry.query_by_field(Instrument, 'identifier',
                                                  instrument_id)
            if not result:
                return True

            instrument = result[0]
            if all([lat_numeric is not None, instrument.y is not None,
                    abs(lat_numeric - instrument.y) >= 1]):
                lat_ok = False
                msg = '#LOCATION.Latitude in file does not match database'
                LOGGER.error(msg)
                self.errors.append((77, msg, values_line))
            if all([lon_numeric is not None, instrument.x is not None,
                    abs(lon_numeric - instrument.x) >= 1]):
                lon_ok = False
                msg = '#LOCATION.Longitude in file does not match database'
                LOGGER.error(msg)
                self.errors.append((77, msg, values_line))
            if all([height_numeric is not None, instrument.z is not None,
                    abs(height_numeric - instrument.z) >= 1]):
                height_ok = False
                msg = '#LOCATION.Height in file does not match database'
                self.warnings.append((77, msg, values_line))

        return all([lat_ok, lon_ok])

    def check_timestamp(self, index):
        table_name = 'TIMESTAMP' if index == 1 else 'TIMESTAMP_' + str(index)
        utcoffset = self.extcsv.extcsv[table_name]['UTCOffset']

        delim = '([^\*])'
        number = '([\d]){1,2}'
        utcoffset_match = re.findall('^(\+|-){num}{delim}{num}{delim}{num}$'
                                     .format(num=number, delim=delim),
                                     utcoffset)

        values_line = self.extcsv.line_num['TIMESTAMP'] + 2
        if len(utcoffset_match) == 1:
            sign, hour, delim1, minute, delim2, second = utcoffset_match[0]

            try:
                magnitude = time(int(hour), int(minute), int(second))
                final_offset = '{}{}'.format(sign, magnitude)
                self.extcsv.extcsv[table_name]['UTCOffset'] = final_offset
                return True
            except (ValueError, TypeError):
                raise
#                msg = 'Improperly formatted #{}.UTCOffset {}' \
#                      .format(table_name, utcoffset)
#                LOGGER.error(msg)
#                self.errors.append((24, msg, values_line))
#                return False

        zero = '([0])'
        utcoffset_match = re.findall('^(\+|-)[0]+{delim}?[0]*{delim}?[0]*$'
                                     .format(delim=delim), utcoffset)
        if len(utcoffset_match) == 1:
            msg = '{}.UTCOffset is a series of zeroes, correcting to' \
                  ' +00:00:00'.format(table_name)
            LOGGER.warning(msg)
            self.warnings.append((23, msg, values_line))

            self.extcsv.extcsv[table_name]['UTCOffset'] = '+00:00:00'
            return True
                
        msg = 'Improperly formatted #{}.UTCOffset {}' \
              .format(table_name, utcoffset)
        LOGGER.error(msg)
        self.errors.append((24, msg, values_line))
        return False

    def check_content_consistency(self):
        dataset = self.extcsv.extcsv['CONTENT']['Category']
        level = self.extcsv.extcsv['CONTENT']['Level']
        form = self.extcsv.extcsv['CONTENT']['Form']

        level_ok = True
        form_ok = True
        values_line = self.extcsv.line_num['CONTENT'] + 2

        if not level:
            if dataset == 'UmkehrN14' and 'C_PROFILE' in self.extcsv.extcsv:
                msg = 'Missing #CONTENT.Level, resolved to 2.0'
                LOGGER.warning(msg)
                self.warnings.append((59, msg, values_line))
                self.extcsv.extcsv['CONTENT']['Level'] = level = 2.0
            else:
                msg = 'Missing #CONTENT.Level, resolved to 1.0'
                LOGGER.warning(msg)
                self.warnings.append((58, msg, values_line))
                self.extcsv.extcsv['CONTENT']['Level'] = level = 1.0
        elif not isinstance(level, float):
            try:
                msg = '#CONTENT.Level = {}, corrected to {}' \
                      .format(level, float(level))
                LOGGER.warning(msg)
                self.warnings.append((57, msg, values_line))
                self.extcsv.extcsv['CONTENT']['Level'] = level = float(level)
            except ValueError:
                msg = 'Invalid characters in #CONTENT.Level'
                LOGGER.error(msg)
                self.errors.append((60, msg, values_line))
                level_ok = False

        if level not in DOMAINS['Datasets'][dataset]:
            msg = 'Unknown #CONTENT.Level for category {}'.format(dataset)
            LOGGER.error(msg)
            self.errors.append((60, msg, values_line))
            level_ok = False

        if not isinstance(form, int):
            try:
                msg = '#CONTENT.Form = {}, corrected to {}' \
                      .format(form, int(form))
                LOGGER.warning(msg)
                self.warnings.append((61, msg, values_line))
                self.extcsv.extcsv['CONTENT']['Form'] = form = int(form)
            except ValueError:
                msg = 'Cannot resolve #CONTENT.Form: is empty or' \
                      ' has invalid characters'
                LOGGER.error(msg)
                self.errors.append((62, msg, values_line))
                form_ok = False

        return level_ok and form_ok

    def check_data_generation_consistency(self):
        date = self.extcsv.extcsv['DATA_GENERATION'].get('Date', None)
        agency = self.extcsv.extcsv['DATA_GENERATION'].get('Agency', None)
        version = self.extcsv.extcsv['DATA_GENERATION'].get('Version', None)

        values_line = self.extcsv.line_num['DATA_GENERATION']

        if not date:
            msg = 'Missing #DATA_GENERATION.Date, resolved to processing date'
            LOGGER.warning(msg)
            self.warnings.append((63, msg, values_line))

            kwargs = {key: getattr(self.process_start, key)
                      for key in ['year', 'month', 'day']}
            today_date = datetime(**kwargs)
            self.extcsv.extcsv['DATA_GENERATION']['Date'] = date = today_date

        try:
            numeric_version = float(version)

            if not 0 <= numeric_version <= 20:
                msg = '#DATA_GENERATION.Version is not within range [0.0]-[20.0]'
                LOGGER.warning(msg)
                self.warnings.append((66, msg, values_line))
            if str(version) == str(int(numeric_version)):
                self.extcsv.extcsv['DATA_GENERATION']['Version'] = numeric_version

                msg = '#DATA_GENERATION.Version corrected to one decimal place'
                LOGGER.warning(msg)
                self.warnings.append((67, msg, values_line))
        except ValueError:
            msg = '#DATA_GENERATION.Version contains invalid characters'
            LOGGER.error(msg)
            self.errors.append((68, msg, values_line))
            return False

        return True

    def finish(self):
        self.registry.close_session()


class ProcessingError(Exception):
    """custom exception handler"""
    pass
