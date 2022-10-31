""" All Data Stores specified in the path"""
import logging
from dataclasses import dataclass
from typing import Optional, List

import gc
import pandas as pd
from joblib import parallel_backend, Parallel, delayed

from project_starter_lib import constants
from project_starter_lib.config import config
from project_starter_lib.data import schemas
from project_starter_lib.data.handlers.common import (
	StorageHandler,
)

logger = logging.getLogger(__name__)


def checks(df, expected_schema):
	"""Checks if the dataframe has all the columns as specified in the schema
	Parameters
	----------
	df :
		Dataframe
	expected_schema :
		Expected schema
	Returns
	-------
	type
		Returns True if all checks are passed. Else, it raises an error with the right message
	"""

	for key, value in expected_schema.items():
		if value != 'str':
			expected_schema[key] = value
		else:
			expected_schema[key] = 'object'

	# Checking that column names are identical
	expected_columns = expected_schema.keys()
	missing_cols = set(expected_columns).difference(df.columns)
	additional_cols = set(df.columns).difference(expected_columns)
	assert len(missing_cols) == 0, f"Missing Columns {missing_cols}"
	assert len(additional_cols) == 0, f"Additional Columns {additional_cols}"

	# Checking that datatypes are identical
	# Get schema of input  df_billed_rev in string format
	df_schema = df.dtypes.apply(lambda x: x.name).to_dict()
	mismatched_cols = {
		key: df_schema[key]
		for key in df_schema
		if df_schema[key] != expected_schema[key]
	}
	assert len(mismatched_cols) == 0, f"Data Types do not match the schema: {mismatched_cols}"

	return True


def clean(df: pd.DataFrame, expected_schema: dict, int_to_string_cols=None):
	"""Cleans the input Data Frame to a expected schema
	Parameters
	----------
	id_cols: Should be a list of cols which are id and should be first converted to integer and then to string
	df :
		Dataframe
	expected_schema :
		Expected schema
	Returns
	-------
	type
		cleaned data frame
		:param expected_schema:
		:param df:
		:param int_to_string_cols:
	"""

	# Drop columns that are not present in expected columns
	expected_columns_list = expected_schema.keys()
	drop_col_list = [x for x in df.columns if x not in expected_columns_list]
	df = df.drop(labels=drop_col_list, axis=1)
	if len(drop_col_list) > 0:
		logger.warning(f"Some columns are being dropped: {drop_col_list}")

	input_dtypes = df.dtypes
	# Changes dtypes to match the schema_dtypes
	for column in input_dtypes.index:
		if (str(input_dtypes[column]) != str(expected_schema[column])) or (
				int_to_string_cols is not None and column in int_to_string_cols):
			logger.info(f"Converting schema for {column} from {input_dtypes[column]} to {expected_schema[column]}")
			# This is done because id cols if specified as object, become int
			if expected_schema[column] == 'object' or (int_to_string_cols is not None and column in int_to_string_cols):
				df[column] = df[column].astype(int, errors='raise').astype(str, errors='raise')
			elif 'datetime' in expected_schema[column]:
				df[column] = pd.to_datetime(df[column], errors="coerce", infer_datetime_format=True)
			else:
				df[column] = df[column].astype(expected_schema[column], errors='raise')
	gc.collect()
	return df


class DataStore:
	""" """

	def __init__(self,
				 file_name,
				 storage_handler: StorageHandler = config.STORAGE_HANDLER,
				 pipeline_name: str = None,
				 task_name: str = None,
				 pipeline_current_run_ids: dict = None,
				 schema: dict = None,
				 flag_copy_to_latest: bool = True,
				 int_to_string_cols: List = None,
				 **kwargs):
		"""
		Initializes a DataStore instance that handles read/write of the intermediate data from/to disk.
		Accessing/setting the `data` property will automatically read/write the data from/to disk.
		Reads will only occur if not already read from disk.
		Parameters
		----------
		storage_handler
			the handler to pull/push data to/from disk
		file_name
			the file name or a list of file names
		pipeline_name
			Pipeline Name with which this datastore is associated with
		task_name
			Task Name with which this datastore is associated with
		schema
			What is the schema of the data
		"""
		self.storage_handler = storage_handler
		self.pipeline_name = pipeline_name
		self.task_name = str(task_name)
		if pipeline_name is None:
			self.read_run_id = None
		else:
			self.read_run_id = config.PIPELINE_READ_RUN_IDs[pipeline_name]
		self.file_name = file_name
		self.pipeline_current_run_ids = pipeline_current_run_ids
		self._data: Optional[pd.DataFrame] = None
		self.schema = schema
		self.flag_copy_to_latest = flag_copy_to_latest
		self.int_to_string_cols = int_to_string_cols
		self.kwargs = kwargs

	def clean_schema(self):
		"""
		Since pandas read_csv cannot directly change datetime schema we have to send it as parse_dates argument
		:return:
		"""

		final_schema = {}
		date_cols = []
		for col_name in self.schema:
			dtype = self.schema[col_name]
			if 'datetime' in dtype:
				date_cols.append(col_name)
			else:
				final_schema[col_name] = dtype

		return final_schema, date_cols

	def create_file_path(self, run_id=None):
		""" Create File Path to read/write from.
		If no pipeline_run_id is present, means we have given an absolute path in the file name
		"""

		if run_id is None:
			path = self.file_name

		else:
			path = f'output_data/{config.ROOT_FOLDER_NAME}/{self.pipeline_name}/{run_id}/{self.task_name}/{self.file_name}'

		return path

	def _load(self):
		path = self.create_file_path(run_id=self.read_run_id)

		# This is done so that we do not have to load the entire dataframe
		if self.schema is not None:
			if path.endswith(".csv"):
				self.kwargs['usecols'] = self.schema.keys()
				self.kwargs['dtype'], self.kwargs['parse_dates'] = self.clean_schema()
			elif path.endswith(".parquet"):
				self.kwargs['columns'] = list(self.schema.keys())

		self._data = self.storage_handler.load(path=path, **self.kwargs)
		if isinstance(self._data, pd.DataFrame):
			logger.info(f"Read {path} with shape: {self._data.shape}")

	@property
	def data(self) -> pd.DataFrame:
		""" While Loading, we have to use the pipeline_run_id that is specified in the configs"""
		# Get the data if not already loaded
		if self._data is None:
			self._load()

		if self.schema is not None:
			try:
				checks(self._data, expected_schema=self.schema)
			except AssertionError:
				logger.warning(f"{self.file_name} has mismatched schema. Trying to clean")
				self._data = clean(self._data, expected_schema=self.schema, int_to_string_cols=self.int_to_string_cols)

		return self._data

	@data.setter
	def data(self, data):

		# This if condition to indicate that we do not want to use the context but directly want to use the file_name
		# as path
		if self.read_run_id is None:
			path_to_save = self.file_name
		else:
			path_to_save = self.create_file_path(run_id=self.pipeline_current_run_ids[self.pipeline_name])

		logger.info(f"Writing file to {path_to_save} with parition_cols {self.kwargs.get('partition_cols')}")

		self.storage_handler.save(path_to_save, data, **self.kwargs)

		# Also save it to the latest folder
		if self.flag_copy_to_latest:
			self.copy_to_latest()
		self._data = data

	@data.deleter
	def data(self):
		self._data = None

	def list_files(self) -> List[str]:
		""" List all files """

		matches = self.storage_handler.list_files(prefix=self.file_name)

		return matches

	def copy_to_latest(self, run_id=None):
		"""
		Copy Files from run_id folder to latest folder
		:param run_id:
		:param file_path:
		:param source_run_id:
		:return:
		"""
		if run_id is None:
			run_id = self.pipeline_current_run_ids[self.pipeline_name]

		# Delete data in latest folder first
		self.storage_handler.delete(path=self.create_file_path(run_id='latest'))
		all_source_files = self.storage_handler.list_files(prefix=self.create_file_path(run_id=run_id))
		with parallel_backend("multiprocessing", n_jobs=16):
			Parallel()(
				delayed(self.storage_handler.copy)(from_path, from_path.replace(run_id, 'latest'))
				for from_path in all_source_files
			)

	def upload_to_cloud(self, local_path):
		"""
		Upload a local location to cloud
		:param local_path:
		:return:
		"""
		path_to_save = self.create_file_path(run_id=self.pipeline_current_run_ids[self.pipeline_name])
		self.storage_handler.upload(local_path=local_path, dest_path=path_to_save)

	def download_from_cloud(self, to_dir, run_id=None):
		"""

		:param run_id:
		:param to_dir:
		:return:
		"""

		if run_id is None:
			run_id = config.PIPELINE_READ_RUN_IDs[self.pipeline_name]

		all_source_files = self.storage_handler.list_files(prefix=self.create_file_path(run_id=run_id))
		all_source_files = [x for x in all_source_files if x.endswith(self.file_name.split(".")[-1])]

		with parallel_backend("multiprocessing", n_jobs=16):
			Parallel()(
				delayed(self.storage_handler.download)(to_dir, from_path, self.file_name)
				for from_path in all_source_files
			)


@dataclass
class AllDataStores:
	"""
	Data Store for all files being generated by the tasks of all pipelines.
	"""
	pipeline_current_ids: dict

	def __post_init__(self):
		""" Ingest Source Data Pipeline """

		# Copy to latest is false for some files because those files are generated in a multi processing fashion.
		# We only want to copy them once all the processes are complete.
		self.ingest_file = DataStore(
			file_name="ingested_file.csv",
			pipeline_name=constants.PIPELINE_INGEST_SOURCE_DATA,
			task_name=constants.TASK_INGEST_FILE,
			pipeline_current_run_ids=self.pipeline_current_ids,
			schema=schemas.INPUT_INGEST_FILE
		)

		self.aggregated_file = DataStore(
			file_name="agg_file.csv",
			pipeline_name=constants.PIPELINE_AGG_DATA,
			task_name=constants.TASK_AGG_FILE,
			pipeline_current_run_ids=self.pipeline_current_ids,
			schema=schemas.INPUT_AGG_FILE
		)