import errno
import os
import shutil
import unittest
from tempfile import mkdtemp

import datetime
import mock

from sdownloader.download import Scenes
from sdownloader.sentinel2 import Sentinel2


class Tests(unittest.TestCase):

    def setUp(self):
        self.temp_folder = mkdtemp()
        self.scenes = ['S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.01',
                       u'S2A_OPER_MSI_L1C_TL_SGS__20160320T140936_A003879_T37TBG_N02.01']
        self.paths = ['tiles/34/R/CS/2016/3/25/0', u'tiles/37/T/BG/2016/3/20/0']

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_folder)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    @mock.patch('sdownloader.download.fetch')
    def test_download_scene_name(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID """

        fake_fetch.side_effect = self._fake_fetch

        l = Sentinel2(download_dir=self.temp_folder)
        results = l.download(self.scenes, [4, 3, 2])

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

        fake_fetch.side_effect = self._fake_fetch

        l = Sentinel2(download_dir=self.temp_folder)
        results = l.download(self.scenes, ['red', 'green', 'blue'])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(self.scenes, results.scenes)
        self.assertEqual(len(results[self.scenes[0]].files), 3)

    @mock.patch('sdownloader.download.fetch')
    def test_download_path(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID """

        fake_fetch.side_effect = self._fake_fetch

        l = Sentinel2(download_dir=self.temp_folder)
        results = l.download(self.paths, [4, 3, 2])

        self.assertTrue(isinstance(results, Scenes))

        total = sum([len(s.files) for s in results])
        self.assertEqual(total, len(self.paths) * 3)

    def test_parse_amazon_s3_path(self):
        self.assertTupleEqual(
            ('56', 'W', 'NV', datetime.date(2016, 5, 30), '0'),
            Sentinel2.parse_amazon_s3_tile_path('tiles/56/W/NV/2016/5/30/0')
        )
        self.assertTupleEqual(
            ('36', 'U', 'YV', datetime.date(2015, 8, 26), '0'),
            Sentinel2.parse_amazon_s3_tile_path('tiles/36/U/YV/2015/8/26/0')
        )
        with self.assertRaises(ValueError):
            Sentinel2.parse_amazon_s3_tile_path('56/W/NV/2016/5/30/0')

    @mock.patch('sdownloader.download.fetch')
    def test_override_relative_path(self, fake_fetch):
        fake_fetch.side_effect = self._fake_fetch

        l = Sentinel2(download_dir=self.temp_folder, relative_product_path_builder=self._custom_relative_path_builder)
        results = l.download(self.paths[1:] + self.scenes[1:], [4, 3, 2])
        self.assertTrue(isinstance(results, Scenes))

        for scene in results:
            for f in scene.files:
                self.assertTrue(f.startswith(os.path.join(self.temp_folder, 'test/37/T/BG/2016-03-20/0/')))

    def _custom_relative_path_builder(self, utm, lat, square, date, seq):
        return os.path.join('test', utm, lat, square, str(date), seq)

    def _fake_fetch(self, url, path, show_progress=False):
        return os.path.join(path, os.path.basename(url))

