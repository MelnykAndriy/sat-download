import abc
import os
import logging

from .common import remote_file_exists, check_create_folder, fetch

logger = logging.getLogger('sdownloader')


class Scene(object):

    def __init__(self, name, files=None):
        self.name = name
        self.files = []

        if isinstance(files, str):
            self.add(files)
        elif isinstance(files, list):
            for f in files:
                self.add(f)

    def add(self, f):
        self.files.append(f)

    def __str__(self):
        return self.name


class Scenes(object):

    def __init__(self, scenes=[]):
        self.scenes_dict = {}
        self.scenes_list = []
        for scene in scenes:
            self.add(self.validate(scene))

    def __getitem__(self, key):
        if isinstance(key, int):
            return self.scenes_list[key]
        elif isinstance(key, (str, unicode)):
            return self.scenes_dict[key]
        else:
            raise Exception('Key is not supported.')

    def __setitem__(self, key, value):
        if isinstance(key, int):
            self.scenes_list[key] = self.validate(value)
        elif isinstance(key, str):
            self.scenes_dict[key] = self.validate(value)
        else:
            raise Exception('Key is not supported.')

    def __len__(self):
        return len(self.scenes_dict.keys())

    def __str__(self):
        return '[Scenes]: Includes %s scenes' % len(self)

    def add(self, scene):
        self.scenes_list.append(self.validate(scene))
        self.scenes_dict[scene.name] = scene

    def add_with_files(self, name, files):
        self.add(Scene(name, files))

    def validate(self, scene):
        if not isinstance(scene, Scene):
            raise Exception('scene must be an instance of Scene')

        return scene

    def merge(self, scenes):
        if not isinstance(scenes, Scenes):
            raise Exception('scenes must be an instance of Scenes')

        for s in scenes:
            self.add(s)

    @property
    def scenes(self):
        return [s.name for s in self]


class S3DownloadMixin(object):

    __metaclass__ = abc.ABCMeta

    def s3(self, scenes, bands):
        """
        Amazon S3 downloader
        """
        if not isinstance(scenes, list):
            raise Exception('Expected scene list')

        scene_objs = Scenes()

        logger.info('Source: AWS S3')
        for scene in scenes:
            files = []

            path = self.scene_interpreter(scene)

            urls = []

            for band in bands:
                # get url for the band
                url = self.amazon_s3_url(path, band)

                # make sure it exist
                remote_file_exists(url)
                urls.append(url)

            folder = os.path.join(self.download_dir, self._relative_product_path(path))
            # create folder
            check_create_folder(folder)

            for url in urls:
                files.append(fetch(url, folder))

            scene_objs.add_with_files(scene, files)

        return scene_objs

    @classmethod
    @abc.abstractmethod
    def scene_interpreter(cls, scene_id):
        pass

    @classmethod
    @abc.abstractmethod
    def amazon_s3_url(cls, path, band):
        pass

    @abc.abstractmethod
    def _relative_product_path(self, scene_id):
        pass

    @abc.abstractproperty
    def download_dir(self):
        pass
