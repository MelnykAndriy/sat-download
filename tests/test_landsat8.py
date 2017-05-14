import errno
import shutil
import tarfile
import unittest
from tempfile import mkdtemp

import mock

from sdownloader.download import Scenes, Scene
from sdownloader.errors import IncorrectLandsat8SceneId, RemoteFileDoesntExist
from sdownloader.landsat8 import Landsat8, GOOGLE_PUBLIC_DATA_STORAGE_SERVICE


class Tests(unittest.TestCase):
    def setUp(self):
        self.temp_folder = mkdtemp()
        self.s3_products = ['LC08_L1TP_128012_20161030_20170318_01_T1', u'LC08_L1TP_035025_20160425_20170223_01_T1',
                            'LC08_L1GT_201113_20160927_20170320_01_T2']
        self.all_scenes = self.s3_products + ['LC08_L1TP_136030_20140713_20170421_01_T1',
                                              'LC08_L1TP_181045_20130619_20170503_01_T1']

        self.band_ids = ['MTL'] + Landsat8._BAND_MAP.values()
        self.real_tar_open = tarfile.open

        self.product_id = 'LC08_L1TP_174037_20170426_20170502_01_T1'
        self.interpreted_product_id = {'path': '174', 'landsat_number': '8', 'sensor': 'C',
                                       'product_id': 'LC08_L1TP_174037_20170426_20170502_01_T1', 'row': '037'}

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_folder)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    @unittest.expectedFailure
    @mock.patch('sdownloader.download.fetch')
    def test_s3(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID """

        fake_fetch.return_value = 'file.tif'

        l = Landsat8(download_dir=self.temp_folder)
        results = l.s3(self.s3_products, [4, 3, 2])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(self.s3_products, results.scenes)
        self.assertEqual(len(results[self.s3_products[0]].files), 3)

    @unittest.expectedFailure
    @mock.patch('sdownloader.download.fetch')
    def test_download_with_band_name(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID with band names """

        fake_fetch.return_value = 'file.tif'

        l = Landsat8(download_dir=self.temp_folder)
        results = l.download(self.s3_products, ['red', 'green', 'blue'])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(self.s3_products, results.scenes)
        self.assertEqual(len(results[self.s3_products[0]].files), 6)

    @unittest.expectedFailure
    @mock.patch('sdownloader.common.download')
    def test_google(self, fake_download):
        fake_download.side_effect = self._landsat_download
        l = Landsat8(download_dir=self.temp_folder)

        bands = [2, 3, 4, 'QA', 'MTL', 'ANG']

        results = l.download(self.all_scenes, service_chain=[GOOGLE_PUBLIC_DATA_STORAGE_SERVICE], bands=bands)

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(len(results), len(self.all_scenes))
        self.assertEqual(fake_download.call_count, len(self.all_scenes) * len(bands))
        self.assertListEqual([r.name for r in results], self.all_scenes)
        for i, scene in enumerate(self.all_scenes):
            self.assertEqual(len(results[scene].files), len(self.band_ids) if bands is None else len(bands))
            for band_filepath in results[scene].files:
                self.assertTrue(band_filepath.startswith(self.temp_folder))

    def _landsat_download(self, _, path, show_progress=False):
        return path

    @mock.patch('sdownloader.landsat8.Landsat8.s3')
    @mock.patch('sdownloader.landsat8.Landsat8._google')
    def test_download_google_when_amazon_is_unavailable(self, fake_google, fake_s3):
        """ Test whether google or amazon are correctly selected based on input """

        fake_google.return_value = Scene(self.all_scenes[-1])
        fake_s3.side_effect = RemoteFileDoesntExist()

        # Test if google is used when an image from 2014 is passed even if bands are provided
        scenes = [self.all_scenes[-1]]
        bands = {2, 3, 4, 5}
        l = Landsat8(download_dir=self.temp_folder)
        l.download(scenes, bands=bands)
        fake_google.assert_called_with(scenes[0], bands.union({'QA', 'ANG', 'MTL'}))

    def test_download_with_unknown_band(self):
        l = Landsat8(download_dir=self.temp_folder)
        with self.assertRaises(IncorrectLandsat8SceneId):
            l.download(self.all_scenes, bands=[432])
        with self.assertRaises(IncorrectLandsat8SceneId):
            l.download(self.all_scenes, bands=['CirrUs'])

    def test_amazon_s3_url(self):
        string = Landsat8.amazon_s3_url(self.interpreted_product_id, 11)
        expect = 'L8/174/037/LC08_L1TP_174037_20170426_20170502_01_T1/LC08_L1TP_174037_20170426_20170502_01_T1_B11.TIF'
        self.assertIn(expect, string)

    def test_google_storage_url(self):
        string = Landsat8.google_storage_url(self.interpreted_product_id, 11)
        expect = 'LC08/01/174/037/LC08_L1TP_174037_20170426_20170502_01_T1/' + \
                 'LC08_L1TP_174037_20170426_20170502_01_T1_B11.TIF'
        self.assertIn(expect, string)

    def test_scene_interpreter(self):
        # Test with correct input
        self.assertDictEqual(self.interpreted_product_id, Landsat8.scene_interpreter(self.product_id))

        # Test with incorrect input
        self.assertRaises(Exception, Landsat8.scene_interpreter, 'LC80030172015001LGN')
