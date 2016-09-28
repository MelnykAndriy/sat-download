import errno
import shutil
import unittest
from tempfile import mkdtemp

import mock

from sdownloader.download import Scenes
from sdownloader.sentinel2 import Sentinel2


class Tests(unittest.TestCase):

    def setUp(self):
        self.temp_folder = mkdtemp()
        self.scenes = ['S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.01',
                       'S2A_OPER_MSI_L1C_TL_SGS__20160320T140936_A003879_T37TBG_N02.01']
        self.paths = ['tiles/34/R/CS/2016/3/25/0', 'tiles/37/T/BG/2016/3/20/0']

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_folder)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    @mock.patch('sdownloader.download.fetch')
    def test_download_scene_name(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID """

        fake_fetch.return_value = 'file.tif'

        l = Sentinel2(download_dir=self.temp_folder)
        results = l.s3(self.scenes, [4, 3, 2])

        self.assertTrue(isinstance(results, Scenes))

        total = sum([len(s.files) for s in results])
        self.assertEqual(total, len(self.scenes) * 3)

    def test_sentinel_scene_interpreter(self):
        expected = 'tiles/56/W/NV/2016/5/30/0'

        scene = 'S2A_tile_20160530_56WNV_0'
        self.assertEqual(Sentinel2.scene_interpreter(scene), expected)

        scene = 'S2A_OPER_MSI_L1C_TL_SGS__20160530T030406_A004890_T56WNV_N01.01'
        self.assertEqual(Sentinel2.scene_interpreter(scene), expected)

    @mock.patch('sdownloader.download.fetch')
    def test_download_with_band_name(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID with band names """

        fake_fetch.return_value = 'file.tif'

        l = Sentinel2(download_dir=self.temp_folder)
        results = l.download(self.scenes, ['red', 'green', 'blue'])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(self.scenes, results.scenes)
        self.assertEqual(len(results[self.scenes[0]].files), 3)

    @mock.patch('sdownloader.download.fetch')
    def test_download_path(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID """

        fake_fetch.return_value = 'file.tif'

        l = Sentinel2(download_dir=self.temp_folder)
        results = l.s3(self.paths, [4, 3, 2])

        self.assertTrue(isinstance(results, Scenes))

        total = sum([len(s.files) for s in results])
        self.assertEqual(total, len(self.paths) * 3)
