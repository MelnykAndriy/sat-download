import logging
import os.path as _path
import re
import shutil as _shutil
import tempfile as _tempfile
import weakref as _weakref
from functools import partial
from os import makedirs

import requests
from homura import download

from .errors import RemoteFileDoesntExist

logger = logging.getLogger('sdownloader')


def check_create_folder(folder_path):
    """ Check whether a folder exists, if not the folder is created.
    :param folder_path:
        Path to the folder
    :type folder_path:
        String
    :returns:
        (String) the path to the folder
    """
    if not _path.exists(folder_path):
        makedirs(folder_path)

    return folder_path


def get_remote_file_size(url):
    """ Gets the filesize of a remote file.
    :param url:
        The url that has to be checked.
    :type url:
        String
    :returns:
        int
    """
    headers = requests.head(url).headers
    return int(headers['content-length'])


def remote_file_exists(url):
        """ Checks whether the remote file exists.
        :param url:
            The url that has to be checked.
        :type url:
            String
        :returns:
            **True** if remote file exists and **False** if it doesn't exist.
        """
        status = requests.head(url).status_code

        if status == 200:
            return True
        else:
            raise RemoteFileDoesntExist


def remove_slash(value):
    """ Removes slash from beginning and end of a string """
    assert isinstance(value, (str, unicode))
    return re.sub('(^\/|\/$)', '', value)


def url_builder(segments):
    """ Join segments with '/' slash to create a path/url """
    # Only accept list or tuple
    assert (isinstance(segments, list) or isinstance(segments, tuple))
    return "/".join([remove_slash(s) for s in segments])


def fetch(url, path, show_progress=False):
    """ Downloads a given url to a give path.
    :param url:
        The url to be downloaded.
    :type url:
        String
    :param path:
        The directory path to where the image should be stored
    :type path:
        String
    :param show_progress:
        Pass true if you want to observe download progress
    :type show_progress:
        bool
    :returns:
        Downloaded file path
    """

    segments = url.split('/')
    filename = segments[-1]

    # remove query parameters from the filename
    filename = filename.split('?')[0]

    if _path.exists(_path.join(path, filename)):
        size = _path.getsize(_path.join(path, filename))
        if size == get_remote_file_size(url):
            logger.info('{0} already exists on your system'.format(filename))

    else:
        download(url, path, show_progress=show_progress)
    logger.info('stored at {0}'.format(path))

    return _path.join(path, filename)


class TemporaryDirectory(object):
    """Create and return a temporary directory.  This has the same
    behavior as mkdtemp but can be used as a context manager.  For
    example:

        with TemporaryDirectory() as tmpdir:
            ...

    Upon exiting the context, the directory and everything contained
    in it are removed.
    """

    def __init__(self, suffix='', prefix='tmp', base_dir=None):
        self.name = _tempfile.mkdtemp(suffix, prefix, base_dir)

        def _callback(cleanup, name, _):
            cleanup(name)

        self._finalizer = _weakref.ref(self, partial(_callback, self._cleanup, self.name))

    @classmethod
    def _cleanup(cls, name):
        if _path.exists(name):
            _shutil.rmtree(name)

    def __repr__(self):
        return "<{} {!r}>".format(self.__class__.__name__, self.name)

    def __enter__(self):
        return self.name

    def __exit__(self, *_):
        self.cleanup()

    def cleanup(self):
        if _path.exists(self.name):
            _shutil.rmtree(self.name)

