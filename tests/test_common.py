import os
import unittest

from mock import mock
from sdownloader.common import fetch

import sdownloader
from sdownloader import common, errors


class Tests(unittest.TestCase):

    def setUp(self):
        self.file_url = 'https://storage.googleapis.com/gcp-public-data-landsat/LC08/01/175/037/' + \
                        'LC08_L1TP_175037_20170503_20170503_01_RT/LC08_L1TP_175037_20170503_20170503_01_RT_B11.TIF'
        self.file_size = 44129543

    @mock.patch('sdownloader.common.download')
    def test_fetch(self, mock_download):
        download_dir = 'dir'
        self.assertTrue(fetch(self.file_url, download_dir))
        mock_download.assert_called_with(self.file_url, download_dir, show_progress=mock.ANY)

    def test_remote_file_size(self):
        size = common.get_remote_file_size(self.file_url)
        self.assertEqual(self.file_size, size)

    def test_remote_file_exist(self):
        # Exists and should return None

        assert common.remote_file_exists(self.file_url)

        # Doesn't exist and should raise errror
        with self.assertRaises(errors.RemoteFileDoesntExist):
            common.remote_file_exists(
                os.path.join(
                    sdownloader.Landsat8.S3_LANDSAT_BASE_URL,
                    'L8/003/017/LC80030172015001LGN00/LC80030172015001LGN00_B34.TIF'
                )
            )

        # Doesn't exist and should raise errror
        with self.assertRaises(errors.RemoteFileDoesntExist):
            common.remote_file_exists(
                os.path.join(
                    sdownloader.Landsat8.GOOGLE_BASE_URL,
                    'L8/003/017/LC80030172015001LGN00/LC80030172015001LGN00_B6.TIF'
                )
            )

        # Exist and shouldn't raise errork
        assert common.remote_file_exists(str.replace(self.file_url, 'B11', 'B8'))

if __name__ == '__main__':
    unittest.main()
