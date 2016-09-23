import logging
from xml.etree import ElementTree

from usgs import api, USGSError

from .download import S3DownloadMixin, Scenes
from .common import (landsat_scene_interpreter, amazon_s3_url_landsat8, check_create_folder,
                     fetch, google_storage_url_landsat8, remote_file_exists)

from .errors import RemoteFileDoesntExist, USGSInventoryAccessMissing

logger = logging.getLogger('sdownloader')

AMAZON_SERVICE = 'amazon'
GOOGLE_CLOUD_SERVICE = 'gcloud'
USGS_SERVICE = 'usgs'


class Landsat8DownloaderException(Exception):
    pass


class Landsat8(S3DownloadMixin):
    """ Landsat8 downloader class """

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
        'quality': 'QA'
    }

    _DEFAULT_BANDS = {'QA', 'MTL'}

    def __init__(self, download_dir, usgs_user=None, usgs_pass=None):
        self.download_dir = download_dir
        self.usgs_user = usgs_user
        self.usgs_pass = usgs_pass
        self.scene_interpreter = landsat_scene_interpreter
        self.amazon_s3_url = amazon_s3_url_landsat8

        # Make sure download directory exist
        check_create_folder(self.download_dir)

    def _band_converter(self, bands=None):
        if bands:
            for band_name_or_id in bands:
                yield self._BAND_MAP[band_name_or_id] if band_name_or_id in self._BAND_MAP else band_name_or_id

    def download(self, scenes, bands=None, service_chain=(AMAZON_SERVICE, GOOGLE_CLOUD_SERVICE, USGS_SERVICE)):
        """
        Download scenes from Google Storage or Amazon S3 if bands are provided
        :param scenes:
            A list of scene IDs
        :type scenes:
            Iterable
        :param bands:
            A list of bands. Default value is None.
        :type scenes:
            List
        :param service_chain:
            A list of service designators to be used for images downloading.
            Also specifies the order.
        :type service_chain:
            Iterable
        :returns:
            (List) includes downloaded scenes as key and source as value (aws or google)
        """
        if isinstance(scenes, list):
            scene_objs = Scenes()

            # Always grab MTL.txt and QA band if bands are specified
            bands = self._DEFAULT_BANDS.union(self._band_converter(bands))

            for scene in scenes:

                for service_designator in service_chain:
                    try:
                        if service_designator == AMAZON_SERVICE:
                            if bands == self._DEFAULT_BANDS:
                                continue

                            scene_objs.merge(self.s3([scene], bands))
                        elif service_designator == GOOGLE_CLOUD_SERVICE:
                            scene_objs.merge(self.google([scene]))
                        elif service_designator == USGS_SERVICE:
                            scene_objs.merge(self.usgs([scene]))
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

    def usgs(self, scenes):
        """
        Downloads the image from USGS
        :param scenes:
            A collection of scene IDs
        :type scenes:
            Iterable
        :returns
            Downloaded scenes wrapper
        """

        if not isinstance(scenes, list):
            raise ValueError('Expected sceneIDs list')

        scene_objs = Scenes()

        # download from usgs if login information is provided
        if self.usgs_user and self.usgs_pass:
            try:
                api_key = api.login(self.usgs_user, self.usgs_pass)
            except USGSError as e:
                error_tree = ElementTree.fromstring(str(e.message))
                error_text = error_tree.find("SOAP-ENV:Body/SOAP-ENV:Fault/faultstring", api.NAMESPACES).text
                raise USGSInventoryAccessMissing(error_text)

            for scene in scenes:
                download_urls = api.download('LANDSAT_8', 'EE', [scene], api_key=api_key)
                if download_urls:
                    logger.info('Source: USGS EarthExplorer')
                    scene_objs.add_with_files(scene, fetch(download_urls[0], self.download_dir))

                else:
                    raise RemoteFileDoesntExist('{0} not available on AWS S3, Google or USGS Earth Explorer'.format(
                                                ' - '.join(scene)))

            return scene_objs

        raise RemoteFileDoesntExist('{0} not available on AWS S3 or Google Storage'.format(' - '.join(scenes)))

    def google(self, scenes):
        """
        Google Storage Downloader.
        :param scenes:
            A collection of scene IDs
        :type scenes:
            Iterable
        :returns:
            Downloaded scenes wrapper
        """

        if not isinstance(scenes, list):
            raise ValueError('Expected sceneIDs list')

        scene_objs = Scenes()
        logger.info('Source: Google Storge')

        for scene in scenes:
            sat = landsat_scene_interpreter(scene)
            url = google_storage_url_landsat8(sat)
            remote_file_exists(url)

            scene_objs.add_with_files(scene, fetch(url, self.download_dir))

        return scene_objs
