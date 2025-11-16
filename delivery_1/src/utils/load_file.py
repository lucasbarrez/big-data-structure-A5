from abc import ABCMeta, abstractmethod
from typing import Callable
from venv import logger
from pathlib import Path
import json

class FileLoaderBase(metaclass=ABCMeta):
    """ Base class for a loader """

    def __init__(self, **kwargs):
        """ Constructor """
        pass

    @abstractmethod
    def load(self, path: str):
        """ Abstract method to load a file based on its path """
        pass
    
    @abstractmethod
    def _check(self, path: str):
        """ Abstract method to check if the file exists and is in the good format """
        pass

class FileLoaderFactory:
    """ The factory class for creating loaders"""

    registry = {}
    """ Internal registry for available loaders """

    @classmethod
    def register(cls, name: str) -> Callable:

        def inner_wrapper(wrapped_class: FileLoaderBase) -> Callable:
            if name in cls.registry:
                logger.warning('Executor %s already exists. Will replace it', name)
            cls.registry[name] = wrapped_class
            return wrapped_class

        return inner_wrapper

@FileLoaderFactory.register('json')
class JsonFileLoader(FileLoaderBase):
    """ Json file loader"""
    def load(self, path: str):
        file_path = Path(path)
        logger.info('Loading %s file ...', path)
    
        try:
            if not self._check(file_path):
                raise Exception("The file does not exist.")
            with open(file_path, 'r') as f:
                file = json.load(f)
            return file
        except Exception as e:
            logger.error(e)
        

    
    def _check(self, file_path: Path):
        return file_path.exists()
        