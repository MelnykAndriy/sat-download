import datetime
import logging
import re

from wordpad import pad

from sdownloader.errors import IncorrectSentine2SceneId
from .download import S3DownloadMixin
from .common import check_create_folder

logger = logging.getLogger('sdownloader')


class Sentinel2(S3DownloadMixin):
    """ Sentinel2 downloader class """

    S3_SENTINEL = 'http://sentinel-s2-l1c.s3.amazonaws.com/'
    _BAND_MAP = {
        'coastal': 1,
        'blue': 2,
        'green': 3,
        'red': 4,
        'nir': 8,
        'swir1': 11,
        'swir2': 12
    }

    def __init__(self, download_dir):
        self.download_dir = download_dir

        # Make sure download directory exist
        check_create_folder(self.download_dir)

    def _band_converter(self, bands=None):
        if bands:
            for i, b in enumerate(bands):
                try:
                    bands[i] = self._BAND_MAP[b]
                except KeyError:
                    pass
        return bands

    def download(self, scenes, bands):
        """
        Download scenese Amazon S3. Bands must be provided

        The scenes could either be a scene_id used by sentinel-api or a s3 path (e.g. tiles/34/R/CS/2016/3/25/0)

        :param scenes:
            A list of scenes
        :type scenes:
            List
        :param bands:
            A list of bands. Default value is None.
        :type scenes:
            List
        :returns:
            (List) includes downloaded scenes as key and source as value (aws or google)
        """
        bands = self._band_converter(bands)

        if isinstance(scenes, list):
            return self.s3(scenes, bands)
        else:
            raise Exception('Expected scene list')

    @classmethod
    def scene_interpreter(cls, scene_name):
        """ This function converts a tile/scene name
        (e.g. S2A_OPER_MSI_L1C_TL_SGS__20160325T150955_A003951_T34RCS_N02.01) to a
        AWS S3 path
        :param scene_name:
        :returns:
        """
        assert isinstance(scene_name, (str, unicode))

        try:
            if '/' in scene_name or 'tiles' in scene_name:
                return scene_name

            splitted = scene_name.split('_')

            if len(splitted) > 5:
                version = int(splitted[-1].split('.')[-1]) - 1
                date = datetime.datetime.strptime(splitted[-4], '%Y%m%dT%H%M%S')

                mgrs = splitted[-2]
                utm = int(mgrs[1:3])
                latitude_band = mgrs[3]
                grid_square = mgrs[4:6]
            else:
                version = int(splitted[-1])
                date = datetime.datetime.strptime(splitted[2], '%Y%m%d')
                pattern = re.compile('(\d+)([A-Z])([A-Z]{2})')
                mgrs = pattern.match(splitted[3])

                if mgrs:
                    utm = int(mgrs.group(1))
                    latitude_band = mgrs.group(2)
                    grid_square = mgrs.group(3)
                else:
                    raise IncorrectSentine2SceneId('Incorrect Scene for Sentinel-2 provided')

            return 'tiles/{0}/{1}/{2}/{3}/{4}/{5}/{6}'.format(
                utm,
                latitude_band,
                grid_square,
                date.year,
                date.month,
                date.day,
                version
            )

        except ValueError:
            raise IncorrectSentine2SceneId('Incorrect Scene for Sentinel-2 provided')

    @classmethod
    def amazon_s3_url(cls, path, band, suffix='B', frmt='jp2'):
        """
        Return an amazon s3 url for a sentinel2 scene band

        :param path:
        :param band:
        :param suffix:
        :param frmt:
        :returns:
            (String) The URL to a S3 file
        """

        return '{0}{1}/{2}{3}.{4}'.format(
            cls.S3_SENTINEL,
            path,
            suffix,
            pad(band, 2),
            frmt
        )
