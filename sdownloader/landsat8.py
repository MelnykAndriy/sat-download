import logging
import os
import tarfile
from xml.etree import ElementTree

from usgs import api, USGSError

from sdownloader.common import url_builder
from sdownloader.errors import IncorrectLandsat8SceneId

from .download import S3DownloadMixin, Scenes, Scene
from .common import (check_create_folder,
                     fetch, remote_file_exists, TemporaryDirectory)

from .errors import RemoteFileDoesntExist, USGSInventoryAccessMissing

logger = logging.getLogger('sdownloader')

AMAZON_SERVICE = 'amazon'
GOOGLE_CLOUD_SERVICE = 'gcloud'
USGS_SERVICE = 'usgs'


class Landsat8DownloaderException(Exception):
    pass


class Landsat8(S3DownloadMixin):
    """ Landsat8 downloader class """

    S3_LANDSAT = 'http://landsat-pds.s3.amazonaws.com/'
    GOOGLE = 'http://storage.googleapis.com/earthengine-public/landsat/'
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

    _DEFAULT_BANDS = {'QA', 'MTL'}

    def __init__(self, download_dir, usgs_user=None, usgs_pass=None):
        self.download_dir = download_dir
        self.usgs_user = usgs_user
        self.usgs_pass = usgs_pass

        # Make sure download directory exist
        check_create_folder(self.download_dir)

    def _band_converter(self, bands=None):
        if bands:
            for band_name_or_id in bands:
                yield self._BAND_MAP[band_name_or_id] if band_name_or_id in self._BAND_MAP else band_name_or_id

    def download(self, scenes, bands=tuple(_BAND_MAP.values()),
                 service_chain=(AMAZON_SERVICE, GOOGLE_CLOUD_SERVICE, USGS_SERVICE)):
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

            for scene_id in scenes:

                for service_designator in service_chain:
                    try:
                        if service_designator == AMAZON_SERVICE:
                            scene_objs.merge(self.s3([scene_id], bands))
                        elif service_designator == GOOGLE_CLOUD_SERVICE:
                            scene_objs.add(self._google(scene_id, bands))
                        elif service_designator == USGS_SERVICE:
                            scene_objs.add(self._usgs(scene_id, bands))
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

    def _usgs(self, scene_id, bands):
        """
        Downloads the image from USGS
        :param scene_id:
            A collection of scene IDs
        :type scene_id:
            Iterable
        :returns
            Downloaded scenes wrapper
        """

        # download from usgs if login information is provided
        if self.usgs_user and self.usgs_pass:
            try:
                api_key = api.login(self.usgs_user, self.usgs_pass)
            except USGSError as e:
                error_tree = ElementTree.fromstring(str(e.message))
                error_text = error_tree.find("SOAP-ENV:Body/SOAP-ENV:Fault/faultstring", api.NAMESPACES).text
                raise USGSInventoryAccessMissing(error_text)

            download_urls = api.download('LANDSAT_8', 'EE', [scene_id], api_key=api_key)
            if download_urls:
                return self._fetch_scene(scene_id, download_urls[0], bands)
            else:
                raise RemoteFileDoesntExist

        raise Landsat8DownloaderException('USGS username and/or password are not provided')

    def _google(self, scene_id, bands):
        """
        Google Storage Downloader.
        :param scene_id:
            Landsat scene ID
        :type scene_id:
            str
        :returns:
            Downloaded scenes wrapper
        """
        sat = self.scene_interpreter(scene_id)
        url = self.google_storage_url(sat)
        remote_file_exists(url)

        return self._fetch_scene(scene_id, url, bands)

    def _fetch_scene(self, scene_id, url, bands):
        with TemporaryDirectory() as temporary_directory:
            return Scene(scene_id, self._extract_bands(fetch(url, temporary_directory),
                                                       os.path.join(self.download_dir, scene_id),
                                                       {self.band_filename(scene_id, band_id) for band_id in bands}))

    @classmethod
    def _extract_bands(cls, scene_archive_path, extract_path, band_filenames):
        extracted = []
        with tarfile.open(scene_archive_path, 'r') as archive:
            for member in archive:
                if member.name in band_filenames:
                    archive.extract(member, extract_path)
                    extracted.append(os.path.join(extract_path, member.name))

        return extracted

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
        filename = cls.band_filename(sat['scene'], band_id)
        return url_builder([cls.S3_LANDSAT, sat['sat'], sat['path'], sat['row'], sat['scene'], filename])

    @classmethod
    def band_filename(cls, scene_id, band_id):
        """

        :param scene_id:
        :param band_id:
        :return:
        """
        if band_id == 'MTL':
            return '{}_{}.txt'.format(scene_id, band_id)
        elif band_id in cls._BAND_MAP.values():
            return '{}_B{}.TIF'.format(scene_id, band_id)
        else:
            raise IncorrectLandsat8SceneId('Provided band id is not correct')

    @classmethod
    def google_storage_url(cls, sat):
        """
        Returns a google storage url for a landsat8 scene.
        :param sat:
            Expects an object created by landsat_scene_interpreter function
        :type sat:
            dict
        :returns:
            (String) The URL to a google storage file
        """
        filename = sat['scene'] + '.tar.bz'
        return url_builder([cls.GOOGLE, sat['sat'], sat['path'], sat['row'], filename])

    @classmethod
    def scene_interpreter(cls, scene_name):
        """ Retrieve row, path and date from Landsat-8 sceneID.
        :param scene_name:
            The scene ID.
        :type scene_name:
            String
        :returns:
            dict
        :Example output:
        >>> anatomy = {
                'path': None,
                'row': None,
                'sat': None,
                'scene': scene
            }
        """

        anatomy = {
            'path': None,
            'row': None,
            'sat': None,
            'scene': scene_name
        }

        if isinstance(scene_name, (str, unicode)) and len(scene_name) == 21:
            anatomy['path'] = scene_name[3:6]
            anatomy['row'] = scene_name[6:9]
            anatomy['sat'] = 'L' + scene_name[2:3]

            return anatomy
        else:
            raise IncorrectLandsat8SceneId('Received incorrect scene')
