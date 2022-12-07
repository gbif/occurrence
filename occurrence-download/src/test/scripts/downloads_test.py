""" Script to run a series of sanity checks for occurrence downloads.
To run this script use python downloads_test.py download_url working_dir, for example:

  python downloads_test.py https://api.gbif-dev.org/v1/occurrence/download/0000442-180611120914121 /tmp/

  - download_url: REST/JSON url to the occurrence download json metadata
  - working_dir: local directory used to download an extract the occurrence download file
"""

import wget, zipfile, os, subprocess, shutil, unittest, requests, sys, inspect, argparse, pandas, untangle
from lxml import etree

#expose the following classes
__all__ = ['BaseDownloadTestCase','CsvDownloadTest','DwcaDownloadTest']


class BaseDownloadTestCase(unittest.TestCase):

  """ Base class for sanity tests of occurrence downloads.
  """
  def __init__(self, download, download_extract_path, download_file, methodName):
    super(BaseDownloadTestCase, self).__init__(methodName)
    self.download = download
    self.download_extract_path = download_extract_path
    self.download_file = download_file

  @staticmethod
  def parametrize(testcase_klass, download, working_dir, download_file):
    """ Create a suite containing all tests taken from the given
        subclass, passing them the parameter 'baseUrl'.
    """
    testloader = unittest.TestLoader()
    testnames = testloader.getTestCaseNames(testcase_klass)
    suite = unittest.TestSuite()
    for name in testnames:
      suite.addTest(testcase_klass(name, download, working_dir, download_file))
    return suite


  def num_lines_in_file(self, fpath):
    """ Counts the number of lines in a file.
    """
    return int(subprocess.check_output('grep -c ^ "%s"' % fpath, shell=True).strip().split()[0])

  def count_last_line_columns(self, fpath):
    """ Read the last line of file.
    """
    return int(subprocess.check_output('tail -1 "%s" | grep -o \'\t\' | wc -l' % fpath, shell=True).strip())

  def check_columns_alignment(self, path_to_file):
    """ Utility method that checks that the header, second and last lines have the same amount of columns.
    """
    with open(path_to_file) as file:
      first_line = file.readline()
      second_line = file.readline()
      first_line_cols = first_line.count('\t')
      second_line_cols = second_line.count('\t')
      last_line_cols = self.count_last_line_columns(path_to_file)
      self.assertTrue(first_line_cols > 0, 'Header or first line is empty')
      self.assertTrue(second_line_cols > 0, 'Second line contains no elements')
      self.assertTrue(last_line_cols > 0, 'Last line in file contains no data')
      self.assertEqual(first_line_cols, second_line_cols, 'First and second lines have different number of columns: {} vs {}'.format(first_line_cols, second_line_cols))
      self.assertEqual(first_line_cols, last_line_cols, 'First and last lines have different number of columns: {} vs {}'.format(first_line_cols, last_line_cols))

  def test_is_successful(self):
    """ Is this a successful download (download.status = SUCCEEDED)?
    """
    self.assertEquals('SUCCEEDED', self.download['status'], 'Not a succeeded download, it is {}'.format(self.download['status']))

  def test_has_datasets(self):
    """ Does this download have at least 1 dataset (download.numberDatasets > 0)?
    """
    self.assertTrue(int(self.download['numberDatasets']) > 0, 'A download must have at least 1 dataset')

  def test_download_size(self):
    """ Does this download have a size greater than 0 (download.size > 0)?
    """
    self.assertTrue(int(self.download['size']) > 0, 'Download size can not be 0')

  def test_has_doi(self):
    """ Does this download have a DOI assigned?
    """
    self.assertTrue(None != self.download['doi'], 'DOI is null')

  def test_has_license(self):
    """ Does this download have a valida License assigned?
    """
    self.assertTrue(None != self.download['license'], 'License is null')
    self.assertTrue('unspecified' != self.download['license'].lower(), 'License has been set to unspecified')

class CsvDownloadTest(BaseDownloadTestCase):

  """ TestCase class to perform sanity checks against a CSV download.
  """
  def __init__(self, methodName, download, download_extract_path, download_file):
    super(CsvDownloadTest, self).__init__(download, download_extract_path, download_file, methodName)
    self.csv_file = os.path.join(self.download_extract_path,os.path.splitext(os.path.basename(self.download_file))[0] + '.csv')

  def test_reported_records_vs_lines(self):
    """ Do the number of reported records and number of lines match?
    """
    num_lines = self.num_lines_in_file(self.csv_file)
    self.assertTrue(num_lines > 1, 'Number of lines can be 0 or 1, actual value {}'.format(num_lines))
    self.assertEquals(num_lines - 1, int(self.download['totalRecords']), 'Number records in json response {}  vs number of lines {} differ'.format(self.download['totalRecords'], num_lines))

  def test_columns_alignment(self):
    """ Have the header, second and last records the same number of columns?
    """
    self.check_columns_alignment(self.csv_file)

class DwcaDownloadTest(BaseDownloadTestCase):

  """ TestCase class to perform sanity checks against a DwCa download.
  """
  def __init__(self, methodName, download, download_extract_path, download_file):
    super(DwcaDownloadTest, self).__init__(download, download_extract_path, download_file, methodName)
    self.intepreted_file = os.path.join(self.download_extract_path, 'occurrence.txt')
    self.verbatim_file = os.path.join(self.download_extract_path, 'verbatim.txt')
    self.meta_xml_file = os.path.join(self.download_extract_path, 'meta.xml')
    self.citations_file = os.path.join(self.download_extract_path, 'citations.txt')
    self.rights_file = os.path.join(self.download_extract_path, 'rights.txt')
    self.metadata_file = os.path.join(self.download_extract_path, 'metadata.xml')

  def test_verbatim_interpreted_lines_count(self):
    """ Verbatim and interpreted file report the same number of records?
    """
    num_int_lines = self.num_lines_in_file(self.intepreted_file)
    num_verb_lines = self.num_lines_in_file(self.verbatim_file)
    self.assertEquals(num_int_lines, num_verb_lines, 'Number of lines in verbatim {} and interpreted {}files differ'.format(num_verb_lines, num_int_lines))

  def test_verbatim_columns_alignment(self):
    """ Have the header, second and last records the same number of columns?
    """
    self.check_columns_alignment(self.verbatim_file)

  def test_interpreted_columns_alignment(self):
    """ Have the header, second and last records the same number of columns?
    """
    self.check_columns_alignment(self.intepreted_file)

  def columns_in_file(self, path_to_file):
    with open(path_to_file) as file:
      return file.readline().count('\t')

  def test_columns_vs_meta_xml(self):
    """ Do the reported number of columns in meta.xml match against the columns in interpreted and verbatim files?
    """
    meta = untangle.parse(self.meta_xml_file)
    verbatim_extension = next(x for x in meta.archive.extension if x.files.location == 'verbatim.txt')
    #get the last field to extract the index number
    num_of_core_cols = int(meta.archive.core.field[-1]['index'])
    num_of_verb_cols = int(verbatim_extension.field[-1]['index'])
    interpreted_nr_columns = self.columns_in_file(self.intepreted_file)
    verbatim_nr_columns = self.columns_in_file(self.verbatim_file)
    self.assertEquals(num_of_core_cols, interpreted_nr_columns, 'Number meta.xml core columns {} differ to number columns in core file {}'.format(num_of_core_cols, interpreted_nr_columns))
    self.assertEquals(num_of_verb_cols, verbatim_nr_columns, 'Number meta.xml core columns {} differ to number columns in core file {}'.format(num_of_verb_cols, verbatim_nr_columns))

  def test_count_eml_files(self):
    """ Do the number of datasets and exported eml files match?
    """
    num_eml_files = len(next(os.walk(os.path.join(self.download_extract_path,'dataset')))[2])
    num_datasets = int(self.download['numberDatasets'])
    self.assertEquals(num_eml_files, num_datasets, 'Number of reported datasets {} and eml files {} differ'.format(num_datasets, num_eml_files))

  def test_citations_count(self):
    """ Has the citations file the right number of records?
    """
    # -1 removes the header
    num_citations_records = self.num_lines_in_file(self.citations_file) - 1
    num_datasets = int(self.download['numberDatasets'])
    self.assertEquals(num_citations_records, num_datasets, 'Number of reported datasets {} and citations entries {} differ'.format(num_datasets, num_citations_records))

  def test_rights_count(self):
    """ Has the rights file the correct number of records?
    """
    num_lines = self.num_lines_in_file(self.rights_file)
    num_rights_records = num_lines if num_lines == 1 else num_lines / 2
    num_datasets = int(self.download['numberDatasets'])
    self.assertEquals(num_rights_records, num_datasets, 'Number of reported datasets {} and rights entries {} differ'.format(num_datasets, num_rights_records))

  def test_extensions_exist(self):
    """ All extensions in meta.xml have existing files?
    """
    meta_xml = etree.parse(self.meta_xml_file)
    for extension_file in meta_xml.xpath('/xmlns:archive/xmlns:extension/xmlns:files/xmlns:location/text()', namespaces={'xmlns': 'http://rs.tdwg.org/dwc/text/'}):
      self.assertTrue(os.path.exists(os.path.join(self.download_extract_path, extension_file)), 'Extension file does not exist')

  def test_requested_extensions_exist(self):
    """ All requested extensions exists in the meta.xml file?
    """
    meta_xml = etree.parse(self.meta_xml_file)
    for extension in self.download.get('verbatimExtensions', []):
      self.assertEquals(1, len(meta_xml.xpath('/xmlns:archive/xmlns:extension[@rowType="' + extension + '"]/xmlns:files/xmlns:location/text()', namespaces={'xmlns': 'http://rs.tdwg.org/dwc/text/'})),
                        'Requested extension no present in meta.xml')

class SpeciesListDownloadTest(BaseDownloadTestCase):

  """ TestCase class to perform sanity checks against a SpeciesList download.
  """
  def __init__(self, methodName, download, download_extract_path, download_file):
    super(SpeciesListDownloadTest, self).__init__(download, download_extract_path, download_file, methodName)
    self.species_list_file = os.path.join(self.download_extract_path,os.path.splitext(os.path.basename(self.download_file))[0] + '.csv')

  def test_columns_alignment(self):
    """ Have the header, second and last records the same number of columns?
    """
    self.check_columns_alignment(self.species_list_file)

  def test_num_of_occurrence_sum(self):
    """ Have the header, second and last records the same number of columns?
    """
    species_data = pandas.read_csv(self.species_list_file,  sep="\t", )
    occurrence_count = species_data['numberOfOccurrences'].sum()
    self.assertTrue(occurrence_count > 0, 'number of occurrences has to be greater than 0')


class TestSuiteBuilder:

  """ Utility class that assemble suite of tests, downloads files and expand them for being used by test cases.
  """
  def __init__(self, download_url, download_file, working_dir='/tmp/'):
    self.download_url = download_url
    self.working_dir = working_dir
    self.downloaded_file = download_file

  def get_download_json(self):
    """ Retrieves the json data of a occurrence download.
    """
    return requests.get(self.download_url).json()

  def download_file(self, download):
    """ Downloads an occurrence download file.
    """
    return wget.download(download['downloadLink'], self.working_dir)

  def num_lines_in_file(self, fpath):
    """ Counts the number of lines in a file.
    """
    return int(subprocess.check_output('wc -l %s' % fpath, shell=True).strip().split()[0])

  def remove_dir(self, path):
    """ Recursively remove a directory.
    """
    if os.path.exists(path):
      shutil.rmtree(path)

  def clean_and_create(self, path):
    """ if exists, removes a directory and then creates it.
    """
    self.remove_dir(path)
    os.mkdir(path)

  def unzip_download(self, zip_file):
    """ Unzips a compressed file.
    """
    extract_path = os.path.join(self.working_dir, os.path.splitext(os.path.basename(zip_file))[0])
    self.clean_and_create(extract_path)
    zip_ref = zipfile.ZipFile(zip_file, 'r')
    zip_ref.extractall(extract_path)
    zip_ref.close()
    return extract_path

  def run_tests(self):
    """ Executes the test suite depending on the type of the download.
    """
    suite = unittest.TestSuite()
    #Get the json data
    download = self.get_download_json()

    #downloads the file
    if self.downloaded_file is None:
      self.downloaded_file = self.download_file(download)

    #extracts the download file in a directory in the working directory
    self.download_extract_path = self.unzip_download(self.downloaded_file)

    #runs a test suite depending on the downlaod format(type)
    if('SIMPLE_CSV' == download['request']['format']):
      suite.addTest(BaseDownloadTestCase.parametrize(CsvDownloadTest, download, self.download_extract_path, self.downloaded_file))
    if('DWCA' == download['request']['format']):
      suite.addTest(BaseDownloadTestCase.parametrize(DwcaDownloadTest, download, self.download_extract_path, self.downloaded_file))
    if('SPECIES_LIST' == download['request']['format']):
      suite.addTest(BaseDownloadTestCase.parametrize(SpeciesListDownloadTest, download, self.download_extract_path, self.downloaded_file))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    suite_builder.tearDown()
    return result


  def tearDown(self):
    """ Removes the worlking directory and the downloaded file.
    """
    self.remove_dir(self.download_extract_path)
    os.remove(self.downloaded_file)


if __name__ == '__main__':

  #arguments parsing
  parser = argparse.ArgumentParser()
  parser.add_argument("url", help="URL to the download to test, i.e. something like: https://api.gbif-dev.org/v1/occurrence/download/0000442-180611120914121")
  parser.add_argument("--file", help="Full path to the downloaded file, if the this script must not download the file")
  parser.add_argument("workingDir", help="Working directory use to download and unzip files", action="store")
  args = parser.parse_args()

  #build and run the test suite
  suite_builder = TestSuiteBuilder(args.url, args.file, args.workingDir)
  result = suite_builder.run_tests()

  #Validates the test results
  if result.testsRun == 0:
    print('WARNING -None tests were executed using {0}'.format(', '.join(sys.argv[1:])))
  if result.wasSuccessful():
    print('OK - All tests passed using {0}'.format(', '.join(sys.argv[1:])))
    sys.exit(0)
  else:
    print('CRITICAL - Tests failed using {0}'.format(', '.join(sys.argv[1:])))
    sys.exit(2)
