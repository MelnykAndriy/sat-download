import logging
import os

from sdownloader.common import url_builder
from sdownloader.errors import IncorrectLandsat8SceneId

from .download import S3DownloadMixin, Scenes, Scene
from .common import check_create_folder, fetch, remote_file_exists

from .errors import RemoteFileDoesntExist

logger = logging.getLogger('sdownloader')

AMAZON_S3_STORAGE = 'amazon'
GOOGLE_PUBLIC_DATA_STORAGE_SERVICE = 'gcloud'


class Landsat8DownloaderException(Exception):
    pass


class Landsat8(S3DownloadMixin):
    """ Landsat8 downloader class """

    S3_LANDSAT_BASE_URL = 'https://s3-us-west-2.amazonaws.com/landsat-pds/c1/'
    GOOGLE_BASE_URL = 'https://storage.googleapis.com/gcp-public-data-landsat/'
    GOOGLE_COLLECTION = '01'
    _BAND_MAP = {
        'coastal': 1,
        'blue': 2,
        'green': 3,
        'red': 4,
        'nir': 5,
        'swir1': 6,
        'swir2': 7,
        'pan': 8,
        'cirrus': 9,
        'tir1': 10,
        'tir2': 11,
        'quality': 'QA'
    }

    _DEFAULT_BANDS = {'QA', 'MTL', 'ANG'}

    def __init__(self, download_dir, relative_product_path_builder=None, show_progress=False):
        self._download_dir = download_dir
        self._relative_product_path_builder = relative_product_path_builder

        self.show_progress = show_progress

        # Make sure download directory exist
        check_create_folder(self.download_dir)

    def _relative_product_path(self, sat):
        if self._relative_product_path_builder:
            return self._relative_product_path_builder(sat)

        return sat['product_id']

    @property
    def download_dir(self):
        return self._download_dir

    def _band_converter(self, bands=None):
        if bands:
            for band_name_or_id in bands:
                yield self._BAND_MAP[band_name_or_id] if band_name_or_id in self._BAND_MAP else band_name_or_id

    def download(self, products, bands=tuple(_BAND_MAP.values()),
                 service_chain=(AMAZON_S3_STORAGE, GOOGLE_PUBLIC_DATA_STORAGE_SERVICE)):
        """
        Download scenes from Google Storage or Amazon S3 if bands are provided
        :param products:
            A list of products IDs
        :type products:
            Iterable
        :param bands:
            A list of bands. Default value is None.
        :type products:
            List
        :param service_chain:
            A list of service designators to be used for images downloading.
            Also specifies the order.
        :type service_chain:
            Iterable
        :returns:
            (List) includes downloaded scenes as key and source as value (aws or google)
        """
        if isinstance(products, list):
            scene_objs = Scenes()

            bands = self._DEFAULT_BANDS.union(self._band_converter(bands))

            for product_id in products:

                for service_designator in service_chain:
                    try:
                        if service_designator == AMAZON_S3_STORAGE:
                            scene_objs.merge(self.s3([product_id], bands))
                        elif service_designator == GOOGLE_PUBLIC_DATA_STORAGE_SERVICE:
                            scene_objs.add(self._google(product_id, bands))
                        else:
                            raise Landsat8DownloaderException(
                                '{} - service designator is not supported'.format(service_designator)
                            )
                        break
                    except RemoteFileDoesntExist:
                        pass
                else:
                    raise RemoteFileDoesntExist

            return scene_objs

        raise ValueError('Expected sceneIDs list')

    def _google(self, product_id, bands):
        """
        Google Storage Downloader.
        :param product_id:
            Landsat product ID
        :type product_id:
            str
        :returns:
            Downloaded scenes wrapper
        """
        sat = self.scene_interpreter(product_id)

        urls = []

        for band in bands:
            # get url for the band
            url = self.google_storage_url(sat, band)

            # make sure it exist
            remote_file_exists(url)
            urls.append(url)

        folder = os.path.join(self.download_dir, self._relative_product_path(sat))
        # create folder
        check_create_folder(folder)

        return Scene(sat['product_id'], [fetch(url, folder, show_progress=self.show_progress) for url in urls])

    @classmethod
    def amazon_s3_url(cls, sat, band_id):
        """
        Return an amazon s3 url for a landsat8 scene band
        :param sat:
            Expects an object created by scene_interpreter function
        :type sat:
            dict
        :param band_id:

        :returns:
            (String) The URL to a S3 file
        """
        filename = cls.band_filename(sat['product_id'], band_id)
        return url_builder(
            [cls.S3_LANDSAT_BASE_URL, 'L' + sat['landsat_number'], sat['path'], sat['row'], sat['product_id'], filename]
        )

    @classmethod
    def google_storage_url(cls, sat, band_id):
        """
        Returns a google storage url for a landsat8 scene.
        :param sat:
            Expects an object created by landsat_scene_interpreter function
        :type sat:
            dict
        :param band_id:

        :returns:
            (String) The URL to a google storage file
        """
        return url_builder(
            [
                cls.GOOGLE_BASE_URL,
                'L{}0{}'.format(sat['sensor'], sat['landsat_number']),
                cls.GOOGLE_COLLECTION,
                sat['path'],
                sat['row'],
                sat['product_id'],
                cls.band_filename(sat['product_id'], band_id)
            ]
        )

    @classmethod
    def band_filename(cls, product_id, band_id):
        """

        :param product_id:
        :param band_id:
        :return:
        """
        if band_id in ['MTL', 'ANG']:
            return '{}_{}.txt'.format(product_id, band_id)
        elif band_id in cls._BAND_MAP.values():
            return '{}_B{}.TIF'.format(product_id, band_id)
        else:
            raise IncorrectLandsat8SceneId('Provided band id is not correct')

    @classmethod
    def scene_interpreter(cls, product_id):
        """ Retrieve row, path and date from Landsat-8 sceneID.
        :param product_id:
            The product ID.
        :type product_id:
            String
        :returns:
            dict
        """
        if isinstance(product_id, (str, unicode)) and len(product_id) == 40:
            return dict(
                path=product_id[10:13],
                row=product_id[13:16],
                landsat_number=product_id[3:4],
                sensor=product_id[1:2],
                product_id=product_id
            )
        else:
            raise IncorrectLandsat8SceneId('Received incorrect scene')
