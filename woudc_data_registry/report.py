
import os
import csv
import logging

import re
from datetime import date
from collections import OrderedDict

from woudc_data_registry import config


LOGGER = logging.getLogger(__name__)


def _group_dict_keys(source):
    """
    Returns a modified versions of the argument, in which tuples
    of keys with shared values map to sets of those values.
    Each value is represented with one entire group as a key.

    Values in the <source> dictionary are lists of hashable values.

    :param source: A `dict` with hashable values.
    :returns: Another `dict` with groups of keys mapping to their
              shared values.
    """

    inverted = {}
    collected = {}

    for key, valuelist in source.items():
        for value in valuelist:
            if value not in inverted:
                inverted[value] = set()
            inverted[value].add(key)

    for value, keylist in inverted.items():
        standard_keylist = tuple(sorted(list(keylist)))

        if standard_keylist not in collected:
            collected[standard_keylist] = set()
        collected[standard_keylist].add(value)

    return collected


class ReportBuilder:
    """
    Manages accounting and file outputs during a processing run.
    Generates several types of report files in the processing run's working
    directory, which are for tracking warnings and errors in the inputs.
    """

    def __init__(self, root, run=0):
        """
        Initialize a new ReportBuilder working in the directory <root>.

        For use in dummy or verification-only runs, passing <root> as None
        causes no output files to be produced.

        While <run> usually stands for the sequence number of a data
        registry processing attempt, giving it special value 0 causes
        it to derive a run number from previous output files in its
        working directory.

        :param root: Path to the processing run's working directory.
        :param run: Sequence number of the current processing attempt in
                    the processing run, or a special value (see above).
        """

        self._working_directory = root
        self._error_definitions = {}
        self._contributors = {
            'unknown': 'UNKNOWN'
        }

        self._contributor_status = {}
        self._report_batch = OrderedDict([
            ('Processing Status', ''),
            ('Error Type', []),
            ('Error Code', []),
            ('Line Number', []),
            ('Message', []),
            ('Dataset', ''),
            ('Data Level', ''),
            ('Data Form', ''),
            ('Agency', ''),
            ('Station Type', ''),
            ('Station ID', ''),
            ('Filename', ''),
            ('Incoming Path', ''),
            ('Outgoing Path', ''),
            ('URN', '')
        ])

        if root is None:
            self._run_number = 0
        else:
            self._run_number = run or self._determine_run_number()

        self.operator_report = None
        self.read_error_definitions(config.WDR_ERROR_CONFIG)

    def _find_operator_reports(self):
        """
        Returns a list of operator report file names that already exist
        in the instance's working directory. If the working directory is
        null, then the list will be empty.

        :returns: List of existing operator report filenames.
        """

        date_pattern = r'\d{4}-\d{2}-\d{2}'
        operator_report_pattern = r'^operator-report-{}-run\d+.csv$' \
                                  .format(date_pattern)

        operator_reports = []
        for filename in os.listdir(self._working_directory):
            if re.match(operator_report_pattern, filename):
                operator_reports.append(filename)

        return operator_reports

    def _determine_run_number(self):
        """
        Returns the next run number that would continue the processing
        run in this report's working directory, based on previous outputs.

        :returns: Next run number in the working directory.
        """

        highest_report_number = 0
        operator_reports = self._find_operator_reports()

        run_number_pattern = r'run(\d+).csv$'
        for operator_report_name in operator_reports:
            match = re.search(run_number_pattern, operator_report_name)
            report_number = int(match.group(1))

            if report_number > highest_report_number:
                highest_report_number = report_number

        return highest_report_number + 1

    def _load_processing_results(self, filepath, contributor,
                                 extcsv=None, data_record=None):
        """
        Pick out relevant values from an incoming file to be written
        with the next operator report.

        :param filepath: Full path to an incoming file.
        :param contributor: Acronym of contributor who submitted the file.
        :param extcsv: Parsed Extended CSV object from the file's contents,
                       or None if the file failed to parse.
        :param data_record: The DataRecord object generated for the incoming
                            file, or None if processing failed.
        :returns: void
        """

        extcsv_to_batch_fields = [
            ('Station Type', 'PLATFORM', 'Type'),
            ('Station ID', 'PLATFORM', 'ID'),
            ('Dataset', 'CONTENT', 'Category'),
            ('Data Level', 'CONTENT', 'Level'),
            ('Data Form', 'CONTENT', 'Form'),
            ('Agency', 'DATA_GENERATION', 'Agency')
        ]

        # Tranfer core file metadata from the Extended CSV to the report batch.
        for batch_field, table_name, extcsv_field in extcsv_to_batch_fields:
            try:
                self._report_batch[batch_field] = \
                    str(extcsv.extcsv[table_name][extcsv_field])
            except (TypeError, KeyError, AttributeError):
                # Some parsing or processing error occurred and the
                # ExtCSV value is unavailable.
                self._report_batch[batch_field] = ''

        if data_record is None:
            self._report_batch['Outgoing Path'] = ''
            self._report_batch['URN'] = ''
        else:
            self._report_batch['Outgoing Path'] = \
                data_record.get_waf_path(config.WDR_WAF_BASEDIR)
            self._report_batch['URN'] = data_record.data_record_id

        self._report_batch['Agency'] = contributor
        self._report_batch['Incoming Path'] = filepath
        self._report_batch['Filename'] = os.path.basename(filepath)

    def _flush_report_batch(self):
        """
        Empties out the stored report batch in preparation for starting to
        process a new file.

        :returns: void
        """

        self.write_operator_report()
        self.write_run_report()

        for field, column in self._report_batch.items():
            if isinstance(column, str):
                self._report_batch[field] = ''
            else:
                self._report_batch[field].clear()

    def _processing_run_statistics(self):
        """
        Returns a summary of passing files, repaired files, and failed files
        per agency throughout the past processing run. Statistics are
        determined using report files in the working directory.

        The summary is a tuple of dictionaries. The first maps agency acronym
        to list of filepaths that passed the first time. The second dictionary
        maps agencies to another dictionary of files to error codes that were
        fixed between runs. The third element is analogous, except the error
        codes were not repaired.

        :returns: A tuple of passes, fixes, and failures among input files.
        """

        passed_files = {}
        files_to_fixes = {}
        files_to_errors = {}

        operator_reports = self._find_operator_reports()
        for report_filename in operator_reports:
            fullpath = os.path.join(self._working_directory, report_filename)

            with open(fullpath) as operator_report:
                local_files_to_errors = {}
                reader = csv.reader(operator_report, escapechar='\\')
                next(reader)

                for line in reader:
                    agency = line[8] or 'UNKNOWN'
                    filename = line[11]

                    status = line[0]
                    error_type = line[1]
                    error_code = int(line[2])
                    msg = line[4]

                    if agency not in local_files_to_errors:
                        local_files_to_errors[agency] = {}
                    if filename not in local_files_to_errors[agency]:
                        local_files_to_errors[agency][filename] = set()

                    if status == 'P':
                        if agency not in passed_files:
                            passed_files[agency] = set()
                        passed_files[agency].add(filename)
                    elif error_type == 'Error' and error_code != 209:
                        # Ignore duplicate version errors if an already-passed
                        # file is accidentally run again.
                        if filename not in passed_files.get(agency, set()):
                            local_files_to_errors[agency][filename].add(msg)

                for agency in files_to_errors:
                    if agency not in passed_files:
                        passed_files[agency] = set()
                    if agency not in files_to_fixes:
                        files_to_fixes[agency] = set()
                    if agency not in local_files_to_errors:
                        local_files_to_errors[agency] = {}

                    # Check for errors fixed in the current report.
                    for filename in files_to_errors[agency]:
                        if filename not in local_files_to_errors[agency]:
                            local_files_to_errors[agency][filename] = set()

                        if filename in passed_files[agency] \
                           and filename in files_to_errors[agency]:
                            if filename not in files_to_fixes[agency]:
                                files_to_fixes[agency][filename] = set()

                            files_to_fixes[agency][filename].update(
                                files_to_errors[agency].pop(filename))

                # Look for new errors from the current report.
                for agency in local_files_to_errors:
                    if agency not in files_to_errors:
                        files_to_errors[agency] = {}

                    for filename in local_files_to_errors[agency]:
                        for error in local_files_to_errors[agency][filename]:
                            if filename not in files_to_errors[agency]:
                                files_to_errors[agency][filename] = set()
                            files_to_errors[agency][filename].add(error)

        return passed_files, files_to_fixes, files_to_errors

    def run_report_filepath(self, run=0):
        """
        Returns a full path to the runport from the <run>'th
        processing attempt in this report's working directory.

        :param run: Optional run number of operator report to look for.
                    If omitted uses the instance's run number.
        :returns: Full path to specified runport.
        """

        run = run or self._run_number
        filename = 'run{}'.format(run)

        if self._working_directory is None:
            return filename
        else:
            return os.path.join(self._working_directory, filename)

    def operator_report_filepath(self, run=0):
        """
        Returns a full path to the operator report from the <run>'th
        processing attempt from today in this report's working directory.

        :param run: Optional run number of operator report to look for.
                    If omitted uses the instance's run number.
        :returns: Full path to specified operator report.
        """

        today = date.today().strftime('%Y-%m-%d')

        run = run or self._run_number
        filename = 'operator-report-{}-run{}.csv'.format(today, run)

        if self._working_directory is None:
            return filename
        else:
            return os.path.join(self._working_directory, filename)

    def email_report_filepath(self):
        """
        Returns a full path to the email report for the processing run
        in this report's working directory.

        :returns: Full path to the processing run's email report.
        """

        today = date.today().strftime('%Y-%m-%d')
        filename = 'failed-files-{}'.format(today)

        if self._working_directory is None:
            return filename
        else:
            return os.path.join(self._working_directory, filename)

    def read_error_definitions(self, filepath):
        """
        Loads the error definitions found in <filepath> to apply those
        rules to future error/warning determination.

        :param filepath: Path to an error definition file.
        :returns: void
        """

        with open(filepath) as error_definitions:
            reader = csv.reader(error_definitions, escapechar='\\')
            next(reader)  # Skip header line.

            for row in reader:
                error_code = int(row[0])
                self._error_definitions[error_code] = row[1:]

    def add_message(self, error_code, line, **kwargs):
        """
        Logs that an error of type <error_code> was found at line <line>
        in an input file.

        Returns two elements: the first is the warning/error message string,
        and the second is False iff <error_code> matches an error severe
        enough to cause a processing run to abort.

        :param error_code: Numeric code of an error, as defined in the
                           error definition file.
        :param line: Line number where the error occurred, or None
                     if not applicable.
        :param kwargs: Keyword parameters to insert into the error message.
        :returns: Error message, and False iff a severe error has occurred.
        """

        try:
            error_class, message_template = self._error_definitions[error_code]
            message = message_template.format(**kwargs)
        except KeyError:
            msg = 'Unrecognized error code {}'.format(error_code)
            LOGGER.error(msg)
            raise ValueError(msg)

        self._report_batch['Line Number'].append(line)
        self._report_batch['Error Code'].append(error_code)
        self._report_batch['Error Type'].append(error_class)
        self._report_batch['Message'].append(message)

        severe = error_class != 'Warning'
        return message, severe

    def record_passing_file(self, filepath, extcsv, data_record):
        """
        Write out all warnings found while processing <filepath>, complete
        with metadata from the source Extended CSV <extcsv> and the
        generated data record <data_record>.

        :param filepath: Path to an incoming data file.
        :param extcsv: Extended CSV object generated from the incoming file.
        :param data_record: Data record generated from the incoming file.
        :returns: void
        """

        contributor = extcsv.extcsv['DATA_GENERATION']['Agency']
        contributor_raw = contributor.replace('-', '').lower()
        self._contributors[contributor_raw] = contributor

        self._load_processing_results(filepath, contributor, extcsv=extcsv,
                                      data_record=data_record)
        self._report_batch['Processing Status'] = 'P'

        if contributor not in self._contributor_status:
            self._contributor_status[contributor] = []
        self._contributor_status[contributor].append(('P', filepath))

        self._flush_report_batch()

    def record_failing_file(self, filepath, contributor, extcsv=None):
        """
        Write out all warnings and errors found while processing <filepath>,
        complete with metadata from the source Extended CSV <extcsv>
        if available. If <extcsv> is None, <agency> must be provided to
        be able to label the file with its contributing agency.

        If the file was able to be parsed, an Extended CSV object was created
        that must be used to determine file metadata. If the file fails to be
        parsed, that metadata will be unavailable and most fields will be
        left blank.

        :param filepath: Path to an incoming data file.
        :param contributor: Acronym of contributor who submitted the file.
        :param extcsv: Extended CSV object generated from the file.
        :returns: void
        """

        self._load_processing_results(filepath, contributor, extcsv=extcsv)
        self._report_batch['Processing Status'] = 'F'

        contributor = self._report_batch['Agency']
        if contributor not in self._contributor_status:
            self._contributor_status[contributor] = []
        self._contributor_status[contributor].append(('F', filepath))

        self._flush_report_batch()

    def write_run_report(self):
        """
        Write a new run report into the working directory (or update an
        existing one), which summarizes which files in the last
        processing attempt passed or failed.

        Files are divided by agency. See processing workflow for more
        information.

        :returns: void
        """

        for contributor in list(self._contributor_status.keys()):
            contributor_raw = contributor.replace('-', '').lower()

            if contributor_raw in self._contributors:
                contributor_official = self._contributors[contributor_raw]

                if contributor != contributor_official:
                    self._contributor_status[contributor_official].extend(
                        self._contributor_status.pop(contributor))
            else:
                if 'UNKNOWN' not in self._contributor_status:
                    self._contributor_status['UNKNOWN'] = []
                self._contributor_status['UNKNOWN'].extend(
                    self._contributor_status.pop(contributor_raw))

        contributor_list = sorted(list(self._contributor_status.keys()))
        if 'UNKNOWN' in contributor_list:
            # Move UNKNOWN to the end of the list, ignoring alphabetical order.
            contributor_list.remove('UNKNOWN')
            contributor_list.append('UNKNOWN')

        blocks = []
        for contributor in contributor_list:
            # List all files processed for each agency along with their status.
            package = contributor + '\n'
            process_results = self._contributor_status[contributor]

            for status, filepath in process_results:
                if status == 'F':
                    package += 'Fail: {}\n'.format(filepath)
                else:
                    package += 'Pass: {}\n'.format(filepath)

            blocks.append(package)

        run_report_path = self.run_report_filepath()
        with open(run_report_path, 'w') as run_report:
            contents = '\n'.join(blocks)
            run_report.write(contents)

    def write_operator_report(self):
        """
        Write a new operator report into the working directory (or update an
        existing one), which contains a summary of errors and warnings
        encountered in all files from the current processing attempt.

        See processing workflow for more information.

        :returns: void
        """

        if self.operator_report is None:
            operator_report_path = self.operator_report_filepath()
            self.operator_report = open(operator_report_path, 'w')

            header = ','.join(self._report_batch.keys())
            self.operator_report.write(header + '\n')

        column_names = ['Line Number', 'Error Type', 'Error Code', 'Message']
        columns = zip(*[self._report_batch[name] for name in column_names])

        for line, err_type, err_code, message in columns:
            line = '' if line is None else str(line)
            row = ','.join([
                self._report_batch['Processing Status'],
                err_type,
                str(err_code),
                line,
                message.replace(',', '\\,'),
                self._report_batch['Dataset'],
                self._report_batch['Data Level'],
                self._report_batch['Data Form'],
                self._report_batch['Agency'],
                self._report_batch['Station Type'],
                self._report_batch['Station ID'],
                self._report_batch['Filename'],
                self._report_batch['Incoming Path'],
                self._report_batch['Outgoing Path'],
                self._report_batch['URN']
            ])

            self.operator_report.write(row + '\n')

    def write_email_report(self, addresses):
        """
        Write an email feedback summary to the working directory.
        The file describes, per agency, how many files in the whole
        processing run failed, were recovered, or passed the first time.

        See processing workflow for more information.

        :param addresses: Map of contributor acronym to email address.
        :returns: void
        """

        passed_files, fixed_files, failed_files = \
            self._processing_run_statistics()

        failed_files_collected = {}
        fixed_files_collected = {}

        for agency, error_map in failed_files.items():
            failed_files_collected[agency] = _group_dict_keys(error_map)
        for agency, error_map in fixed_files.items():
            fixed_files_collected[agency] = _group_dict_keys(error_map)

        agencies = set(passed_files.keys()) | set(fixed_files.keys()) \
            | set(failed_files.keys())
        sorted_agencies = sorted(list(agencies))

        if 'UNKNOWN' in sorted_agencies:
            # Move UNKNOWN to be always at the end of the report.
            sorted_agencies.remove('UNKNOWN')
            sorted_agencies.append('UNKNOWN')

        email_report_path = self.email_report_filepath()
        with open(email_report_path, 'w') as email_report:
            blocks = []

            for agency in sorted_agencies:
                passed_count = len(passed_files.get(agency, {}))
                fixed_count = len(fixed_files.get(agency, {}))
                failed_count = len(failed_files.get(agency, {}))

                total_count = passed_count + fixed_count + failed_count

                if agency in addresses:
                    email = addresses[agency]
                    header = '{} ({})'.format(agency, email)
                else:
                    header = agency

                feedback = '{}\n' \
                    'Total files received: {}\n' \
                    'Number of passed files: {}\n' \
                    'Number of manually repaired files: {}\n' \
                    'Number of failed files: {}\n' \
                    .format(header, total_count, passed_count,
                            fixed_count, failed_count)

                if failed_count > 0:
                    fail_summary = 'Summary of Failures:\n'

                    for filelist in failed_files_collected[agency]:
                        errorlist = failed_files_collected[agency][filelist]
                        fail_summary += '\n'.join(errorlist) + '\n'
                        fail_summary += '\n'.join(sorted(filelist)) + '\n'

                    feedback += fail_summary

                if fixed_count > 0:
                    fix_summary = 'Summary of Fixes:\n'

                    for filelist in fixed_files_collectied[agency]:
                        errorlist = fixed_files_collectied[agency][filelist]
                        fix_summary += '\n'.join(errorlist) + '\n'
                        fix_summary += '\n'.join(sorted(filelist)) + '\n'

                    feedback += fix_summary

                blocks.append(feedback)

            email_report.write('\n'.join(blocks))

    def close(self):
        """
        Release held resources and file descriptors.

        :returns: void
        """

        self.operator_report.close()
