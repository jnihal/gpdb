import imp
import os
from mock import Mock, patch
from gppylib.test.unit.gp_unittest import GpTestCase,run_tests

class GpCheckPerf(GpTestCase):
    def setUp(self):
        gpcheckcat_file = os.path.abspath(os.path.dirname(__file__) + "/../../../gpcheckperf")
        self.subject = imp.load_source('gpcheckperf', gpcheckcat_file)

    def tearDown(self):
        super(GpCheckPerf, self).tearDown()

    @patch('gpcheckperf.getPlatform', return_value='darwin')
    @patch('gpcheckperf.run')
    def test_get_memory_on_darwin(self, mock_run, mock_get_platform):
        mock_run.return_value = [1, 'hw.physmem: 1234']
        actual_result = self.subject.getMemory()
        self.assertEquals(actual_result, -1)

        mock_run.return_value = [0, 'hw.physmem: 1234']
        actual_result = self.subject.getMemory()
        self.assertEquals(actual_result, 1234)

    @patch('gpcheckperf.getPlatform', return_value='linux')
    @patch('gpcheckperf.run')
    def test_get_memory_on_linux(self, mock_run, mock_get_platform):
        mock_run.return_value = [1, 'MemTotal:        10 kB']
        actual_result = self.subject.getMemory()
        self.assertEquals(actual_result, -1)

        mock_run.return_value = [0, 'MemTotal:        10 kB']
        actual_result = self.subject.getMemory()
        self.assertEquals(actual_result, 10240)

    @patch('gpcheckperf.getPlatform', return_value='abc')
    def test_get_memory_on_invalid_platform(self, mock_get_platform):
        actual_result = self.subject.getMemory()
        self.assertEquals(actual_result, -1)


if __name__ == '__main__':
    run_tests()
