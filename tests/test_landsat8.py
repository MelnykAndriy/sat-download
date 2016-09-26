import errno
import shutil
import tarfile
import unittest
from tempfile import mkdtemp

import io
import mock

from sdownloader.download import Scenes, Scene
from sdownloader.errors import IncorrectLandsat8SceneId, RemoteFileDoesntExist
from sdownloader.landsat8 import Landsat8, USGS_SERVICE, GOOGLE_CLOUD_SERVICE


class Tests(unittest.TestCase):

    def setUp(self):
        self.temp_folder = mkdtemp()
        self.s3_scenes = ['LC80010092015051LGN00', 'LC82050312015136LGN00']
        self.all_scenes = ['LC80010092015051LGN00', 'LC82050312015136LGN00', 'LT81360082013127LGN01',
                           'LC82050312014229LGN00']
        self.scene_size = 59204484

        self.band_ids = ['MTL'] + Landsat8._BAND_MAP.values()
        self.real_tar_open = tarfile.open

    def tearDown(self):
        try:
            shutil.rmtree(self.temp_folder)
        except OSError as exc:
            if exc.errno != errno.ENOENT:
                raise

    @mock.patch('sdownloader.download.fetch')
    def test_s3(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID """

        fake_fetch.return_value = 'file.tif'

        l = Landsat8(download_dir=self.temp_folder)
        results = l.s3(self.s3_scenes, [4, 3, 2])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(self.s3_scenes, results.scenes)
        self.assertEqual(len(results[self.s3_scenes[0]].files), 3)

    @mock.patch('sdownloader.download.fetch')
    def test_download_with_band_name(self, fake_fetch):
        """ Test downloading from S3 for a given sceneID with band names """

        fake_fetch.return_value = 'file.tif'

        l = Landsat8(download_dir=self.temp_folder)
        results = l.download(self.s3_scenes, ['red', 'green', 'blue'])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(self.s3_scenes, results.scenes)
        self.assertEqual(len(results[self.s3_scenes[0]].files), 5)

    def test_google_all_bands(self):
        """ Test downloading from google for a given sceneID """
        self._test_service_download_bands(service=GOOGLE_CLOUD_SERVICE)

    def test_google_specific_bands(self):
        self._test_service_download_bands(service=GOOGLE_CLOUD_SERVICE, bands=[2, 3, 4, 'QA', 'MTL'])

    @mock.patch('sdownloader.landsat8.api.download')
    def test_usgs_all_bands(self, fake_api_download):
        """ Test downloading from google for a given sceneID """

        fake_api_download.side_effect = self._usgs_api_download

        self._test_service_download_bands(service=USGS_SERVICE)

    @mock.patch('sdownloader.landsat8.api.download')
    def test_usgs_download_specific_bands(self, fake_api_download):
        fake_api_download.side_effect = self._usgs_api_download

        self._test_service_download_bands(service=USGS_SERVICE, bands=[8, 9, 10, 11, 'QA', 'MTL'])

    def _usgs_api_download(self, dataset, node, scene_ids, product='STANDARD', api_key=None):
        return ['example.com/{}.tar.bz'.format(scene_id) for scene_id in scene_ids]

    @mock.patch('tarfile.open')
    @mock.patch('sdownloader.common.download')
    @mock.patch('sdownloader.landsat8.api.login')
    def _test_service_download_bands(self, fake_login, fake_download, fake_tar_open, service, bands=None):
        fake_login.return_value = True
        fake_download.side_effect = self._landsat_download
        fake_tar_open.side_effect = self._create_landsat_archive
        l = Landsat8(download_dir=self.temp_folder, usgs_user='test', usgs_pass='test')

        if bands is not None:
            results = l.download(self.all_scenes, service_chain=[service], bands=bands)
        else:
            results = l.download(self.all_scenes, service_chain=[service])

        self.assertTrue(isinstance(results, Scenes))
        self.assertEqual(len(results), len(self.all_scenes))
        self.assertEqual(fake_download.call_count, len(self.all_scenes))
        self.assertEqual(fake_tar_open.call_count, len(self.all_scenes))
        self.assertListEqual([r.name for r in results], self.all_scenes)
        for i, scene in enumerate(self.all_scenes):
            self.assertEqual(len(results[scene].files), len(self.band_ids) if bands is None else len(bands))
            for band_filepath in results[scene].files:
                self.assertTrue(band_filepath.startswith(self.temp_folder))

    def _landsat_download(self, _, path):
        return path

    def _create_landsat_archive(self, path, *args, **kwargs):
        fake_landsat_archive_content = io.BytesIO()
        scene_id = path.split('/')[-1].split('.')[0]
        with self.real_tar_open(fileobj=fake_landsat_archive_content, mode='w:gz') as tar:
            for band_id in self.band_ids:
                tar.addfile(
                    tarfile.TarInfo(Landsat8.band_filename(scene_id, band_id)),
                    io.BytesIO()
                )

        fake_landsat_archive_content.seek(0)
        return self.real_tar_open(fileobj=fake_landsat_archive_content, mode='r:gz')

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
        fake_google.assert_called_with(scenes[0], bands.union({'QA', 'MTL'}))

    def test_download_with_unknown_band(self):
        l = Landsat8(download_dir=self.temp_folder)
        with self.assertRaises(IncorrectLandsat8SceneId):
            l.download(self.all_scenes, bands=[432])
        with self.assertRaises(IncorrectLandsat8SceneId):
            l.download(self.all_scenes, bands=['CirrUs'])