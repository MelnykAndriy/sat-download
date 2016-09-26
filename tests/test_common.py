import json
import os
import errno
import pickle
import shutil
import unittest
from tempfile import mkdtemp

import gc
import mock

import sdownloader
from sdownloader import common, errors


class Tests(unittest.TestCase):

    def setUp(self):
        self.temp_folder = mkdtemp()
        self.scene = 'LT81360082013127LGN01'
        self.scene_2 = 'LC82050312014229LGN00'
        self.scene_s3 = 'LC80010092015051LGN00'
        self.scene_s3_2 = 'LC82050312015136LGN00'
        self.scene_size = 59239149

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_folder)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    def assertSize(self, url, path):
        remote_size = self.d.get_remote_file_size(url)
        download_size = os.path.getsize(path)

        self.assertEqual(remote_size, download_size)

    @mock.patch('sdownloader.common.download')
    def test_fetch(self, mock_download):
        mock_download.return_value = True

        sat = sdownloader.Landsat8.scene_interpreter(self.scene)
        url = sdownloader.Landsat8.google_storage_url(sat)

        self.assertTrue(common.fetch(url, self.temp_folder))

    def test_remote_file_size(self):

        url = sdownloader.Landsat8.google_storage_url(
            sdownloader.Landsat8.scene_interpreter(self.scene))
        size = common.get_remote_file_size(url)

        self.assertEqual(self.scene_size, size)

    def test_google_storage_url(self):
        sat = sdownloader.Landsat8.scene_interpreter(self.scene)

        string = sdownloader.Landsat8.google_storage_url(sat)
        expect = 'L8/136/008/LT81360082013127LGN01.tar.bz'
        assert expect in string

    def test_amazon_s3_url(self):
        sat = sdownloader.Landsat8.scene_interpreter(self.scene)
        string = sdownloader.Landsat8.amazon_s3_url(sat, 11)
        expect = 'L8/136/008/LT81360082013127LGN01/LT81360082013127LGN01_B11.TIF'
        assert expect in string

    def test_amazon_s3_url_sentinel2(self):
        scene = 'S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.01'
        path = sdownloader.Sentinel2.scene_interpreter(scene)
        string = sdownloader.Sentinel2.amazon_s3_url(path, 11)
        expect = 'tiles/34/R/CS/2016/3/25/0/B11.jp2'
        assert expect in string

    def test_remote_file_exist(self):
        # Exists and should return None

        assert common.remote_file_exists(os.path.join(sdownloader.Landsat8.S3_LANDSAT, 'L8/003/017/LC80030172015001L'
                                                      'GN00/LC80030172015001LGN00_B6.TIF'))

        # Doesn't exist and should raise errror
        with self.assertRaises(errors.RemoteFileDoesntExist):
            common.remote_file_exists(
                os.path.join(
                    sdownloader.Landsat8.S3_LANDSAT,
                    'L8/003/017/LC80030172015001LGN00/LC80030172015001LGN00_B34.TIF'
                )
            )

        # Doesn't exist and should raise errror
        with self.assertRaises(errors.RemoteFileDoesntExist):
            common.remote_file_exists(
                os.path.join(
                    sdownloader.Landsat8.GOOGLE,
                    'L8/003/017/LC80030172015001LGN00/LC80030172015001LGN00_B6.TIF'
                )
            )

        # Exist and shouldn't raise error
        assert common.remote_file_exists(os.path.join(sdownloader.Landsat8.GOOGLE, 'L8/003/017/LC80030172015001LGN00.tar.bz'))

    def test_scene_interpreter(self):
        # Test with correct input
        scene = 'LC80030172015001LGN00'
        ouput = sdownloader.Landsat8.scene_interpreter(scene)
        self.assertEqual({'path': '003', 'row': '017', 'sat': 'L8', 'scene': scene}, ouput)

        # Test with incorrect input
        self.assertRaises(Exception, sdownloader.Landsat8.scene_interpreter, 'LC80030172015001LGN')

    def test_scene_interpreter_success(self):
        scene = 'S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.01'
        output = sdownloader.Sentinel2.scene_interpreter(scene)
        expect = 'tiles/34/R/CS/2016/3/25/0'
        self.assertEqual(output, expect)

        scene = 'S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.01'
        output = sdownloader.Sentinel2.scene_interpreter(scene)
        expect = 'tiles/34/R/CS/2016/3/25/0'
        self.assertEqual(output, expect)

        scene = 'S2A_tile_20160526_1VCH_0'
        output = sdownloader.Sentinel2.scene_interpreter(scene)
        expect = 'tiles/1/V/CH/2016/5/26/0'
        self.assertEqual(output, expect)

    def test_scene_interpreter_fail(self):
        with self.assertRaises(errors.IncorrectSentine2SceneId):
            scene = 'S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.what'
            sdownloader.Sentinel2.scene_interpreter(scene)


class TemporaryDirectoryTest(unittest.TestCase):

    def test_context_manager_create(self):

        with common.TemporaryDirectory() as tmpdir_name:
            self.assertTrue(os.path.exists(tmpdir_name))
            self.assertTrue(os.path.isdir(tmpdir_name))
            directory_path = tmpdir_name

        self.assertFalse(os.path.exists(directory_path))

    def test_manual_create_and_delete(self):
        tmpdir = common.TemporaryDirectory()
        self.assertTrue(os.path.exists(tmpdir.name))
        tmpdir.cleanup()
        self.assertFalse(os.path.exists(tmpdir.name))

    def _create_tmp_dir(self):
        tmpdir = common.TemporaryDirectory()
        self.assertTrue(os.path.exists(tmpdir.name))
        return tmpdir.name

    def test_manual_create_without_delete(self):
        directory_path = self._create_tmp_dir()
        gc.collect()
        self.assertFalse(os.path.exists(directory_path))

    def test_temporary_directory_operations(self):
        json_filename = 'obj.json'
        bin_filename = 'obj.dat'
        with common.TemporaryDirectory() as tmpdir_name:
            obj = {'x': 1, 'y': 2}
            json_file_path = os.path.join(tmpdir_name, json_filename)
            bin_file_path = os.path.join(tmpdir_name, bin_filename)

            with open(json_file_path, 'w') as f:
                json.dump(obj, f)

            with open(bin_file_path, 'wb') as f:
                pickle.dump(obj, f)

            self.assertListEqual(sorted([json_filename, bin_filename]), sorted(os.listdir(tmpdir_name)))
            self.assertDictEqual(obj, json.load(open(json_file_path, 'r')))
            self.assertDictEqual(obj, pickle.load(open(bin_file_path, 'rb')))

        self.assertFalse(os.path.exists(json_file_path))
        self.assertFalse(os.path.exists(bin_file_path))


if __name__ == '__main__':
    unittest.main()
