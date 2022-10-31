""" Base class handlers handler inherited by others"""

from abc import abstractmethod
from typing import Union, List

import pandas as pd

""" Set handler variables to be used throughout the codebase"""


class StorageHandler:
	""" """

	@abstractmethod
	def list_files(self, prefix: str, suffix: str) -> List[str]:
		pass

	@abstractmethod
	def load(self, path: str, **kwargs) -> pd.DataFrame:
		pass

	@abstractmethod
	def save(self, path: str, data: Union[pd.DataFrame, str], **kwargs):
		pass

	@abstractmethod
	def copy(self, source_location: str, dest_location: str, **kwargs):
		pass

	@abstractmethod
	def upload(self, local_path: str, dest_path: str, **kwqrgs):
		""" Upload a file from local to location"""
		pass

	@abstractmethod
	def delete(self, path: str, **kwargs):
		""" Upload a file from local to location"""
		pass

	def download(self, to_dir: str, from_path: str, file_name: str, **kwargs):
		""" Download a file to local"""
		pass
