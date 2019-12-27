
import os
import csv
import logging

from datetime import date

from woudc_data_registry import config


LOGGER = logging.getLogger(__name__)


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

        self._report_batch = {
            'Processing Status': '',
            'Station Type': '',
            'Station ID': '',
            'Agency': '',
            'Dataset': '',
            'Data Level': '',
            'Data Form': '',
            'Filename': '',
            'Line Number': [],
            'Error Type': [],
            'Error Code': [],
            'Message': [],
            'Incoming Path': '',
            'Outgoing Path': '',
            'URN': ''
        }

        if root is None:
            self._run_number = 0
        else:
            self._run_number = run or self._determine_run_number()

        self.read_error_definitions(config.WDR_ERROR_CONFIG)

    def _determine_run_number(self):
        """
        Returns the next run number that would continue the processing
        run in this report's working directory, based on previous outputs.

        :returns: Next run number in the working directory.
        """

        run_number = 1

        # Count run reports in the working directory.
        while os.path.exists(self.run_report_filepath(run_number)):
            run_number += 1

        return run_number

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

    def run_report_filepath(self, run=0):
        """
        Returns a full path to the runport from the <run>'th
        processing attempt in this report's working directory.

        :param run: Optional run number of operator report to look for.
                    If omitted uses the instance's run number.
        :returns: Full path to specified runport.
        """

        filename = 'run{}'.format(self._run_number)

        if self._working_dir is None:
            return filename
        else:
            return os.path.join(self._working_dir, filename)

    def operator_report_filepath(self, run=0):
        """
        Returns a full path to the operator report from the <run>'th
        processing attempt from today in this report's working directory.

        :param run: Optional run number of operator report to look for.
                    If omitted uses the instance's run number.
        :returns: Full path to specified operator report.
        """

        today = date.today().strftime('%Y-%m-%d')
        filename = 'operator-report-{}-run{}.csv'.format(today,
                                                         self._run_number)
        if self._working_dir is None:
            return filename
        else:
            return os.path.join(self._working_dir, filename)

    def email_report_filepath(self):
        """
        Returns a full path to the email report for the processing run
        in this report's working directory.

        :returns: Full path to the processing run's email report.
        """

        today = date.today().strftime('%Y-%m-%d')
        filename = 'failed-files-{}'.format(today)

        if self._working_dir is None:
            return filename
        else:
            return os.path.join(self._working_dir, filename)

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

        self._flush_report_batch()

    def record_failing_file(self, filepath, extcsv=None, agency=None):
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
        :param extcsv: Extended CSV object generated from the incoming file.
        :param agency: Agency acronym responsible for the incoming file.
        :returns: void
        """

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

        pass

    def write_operator_report(self):
        """
        Write a new operator report into the working directory (or update an
        existing one), which contains a summary of errors and warnings
        encountered in all files from the current processing attempt.

        See processing workflow for more information.

        :returns: void
        """

        pass

    def write_email_report(self, addresses):
        """
        Write an email feedback summary to the working directory.
        The file describes, per agency, how many files in the whole
        processing run failed, were recovered, or passed the first time.

        The email summary is generated from the operator reports in the
        working directory. That is, the operator reports determine
        what counts as an error or a pass or a fail.

        See processing workflow for more information.

        :param addresses: Map of contributor acronym to email address.
        :returns: void
        """

        pass
