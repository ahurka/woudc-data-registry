
import logging


LOGGER = logging.getLogger(__name__)


class ReportBuilder:
    """
    Manages accounting and file outputs during a processing run.
    Generates several types of report files in the processing run's working
    directory, which are for tracking warnings and errors in the inputs.
    """

    def __init__(self, root):
        """
        Initialize a new ReportBuilder working in the directory <root>.

        :param root: Path to the processing run's working directory.
        """

        self._working_directory = root

    def add_message(self, error_code, line, **kwargs):
        """
        Logs that an error of type <error_code> was found at line <line>
        in an input file.

        Returns False iff <error_code> matches an error severe enough
        to abort a processing run.

        :param error_code: Numeric code of an error, as defined in the
                           error definition file.
        :param line: Line number where the error occurred, or None
                     if not applicable.
        :param kwargs: Keyword parameters to insert into the error message.
        :returns: False iff a severe error has occurred.
        """

        return True

    def record_passing_file(self, filepath, extcsv, data_record):
        """
        Write out all warnings found while processing <filepath>, complete
        with metadata from the source Extended CSV <extcsv> and the
        generated data record <data_record>.

        :param filepath: Path to an incoming data file.
        :param extcsv: Extended CSV object generated from the incoming file.
        :param data_record: Data record generated from the incoming file.
        """

        pass

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
        """

        pass
