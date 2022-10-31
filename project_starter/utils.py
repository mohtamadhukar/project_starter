""" Some common util functions that can be used irrespective of pipelines """
import glob
import json
import logging
import os
import shutil
from typing import List

import numpy as np
import pandas as pd
from joblib import parallel_backend, Parallel, delayed

import project_starter.data.data_stores as data_stores
from project_starter.config import config

logger = logging.getLogger(__name__)


def convert_week_date(data: pd.Series, target: str) -> pd.Series:
	"""
    This function converts a date column into another date column
    where each element is mapped into a specific week day such as Sunday
    Parameters
    ----------
    data: pd.Series
        a column in df that is in the format is pd.datetime
    target: str
        the weekdate (Sunday) we want to shift all elements to.
        should be "Monday", "Tuesday", etc
    Returns
    -------
        a pd.Series that is a date column all the dates converted to a specified weekday.
    """
	mapping = {
		"Monday"   : 0,
		"Tuesday"  : 1,
		"Wednesday": 2,
		"Thursday" : 3,
		"Friday"   : 4,
		"Saturday" : 5,
		"Sunday"   : 6,
	}

	assert target in mapping.keys(), "target should be a week day, e.g., 'Tuesday'"

	# shift the dates
	data = pd.to_datetime(data)

	converted_data = data.where(
		data
		== (
				data
				+ pd.tseries.offsets.Week(weekday=mapping[target])
				- pd.tseries.offsets.Week()
		),
		data + pd.tseries.offsets.Week(weekday=mapping[target]),
	)
	return converted_data


def read_all_new_data(prefix: str, schema: dict, incremental_files: List, **kwargs):
	"""
    Read in all the csv files which prefix returns and return a combined dataframe
    :param incremental_files:
    :param schema: Schema of the csv file
    :param prefix: Prefix name of the file to search for
    :return: combined dataframe or None if no new file found
    """

	files_to_store = [
		f'{prefix}_{file_name}.csv' for file_name in incremental_files
	]

	if len(files_to_store) == 0:
		logger.warning("No new  file found.")
		return None

	# Aggregating all new data
	else:
		logger.info(f"New DF will be created from {files_to_store}")

		def multi_processing_func(file_path, schema, **kwargs):
			df = data_stores.DataStore(file_name=file_path, schema=json.loads(schema), **kwargs).data
			return df

		with parallel_backend("threading", n_jobs=-1):
			new_file_dataframes = Parallel()(
				delayed(multi_processing_func)(filename, json.dumps(schema), **kwargs)
				for filename in files_to_store)

		df_new = pd.concat(new_file_dataframes, axis=0, ignore_index=True)
	return df_new


def decorate_run_pipeline(method):
	"""
    Some housekeeping stuff to do before running a pipeline. Needs to be called as a decorator on each run_pipeline
    run
    """

	def decorate_run_pipeline_inner(pipeline_object, *args, **kwargs):
		""" Pipeline wrapper """
		logger.info(f"======={pipeline_object.name} Started with run_id {pipeline_object.current_run_id}=======")

		# Gathering some information about the pipeline and saving it
		output_dict = {
			'git_branch'       : config.GIT_BRANCH,
			'git_hash'         : config.GIT_HASH,
			'config': json.dumps(config.cfg.export(), indent=4, sort_keys=True, default=str),
		}

		output_file_path = f'output_data/{config.ROOT_FOLDER_NAME}/{pipeline_object.name}/' \
						   f'{pipeline_object.current_run_id}/pipeline_run_info/run_info.json'

		output_file = data_stores.DataStore(
			file_name=output_file_path,
			flag_copy_to_latest=False
		)
		output_file.data = output_dict

		# Running the pipeline
		value = method(pipeline_object, *args, **kwargs)

		logger.info(f"{pipeline_object.name} Completed")

		return value

	return decorate_run_pipeline_inner


def delete_directory(path: str):
	"""
    Delete directory, sub-directory and files in them on local
    :param path:
    :return:
    """

	for clean_up in glob.glob(f'{path}/*.*'):
		if not clean_up.endswith('.gitkeep'):
			try:
				os.remove(clean_up)
			except IsADirectoryError:
				shutil.rmtree(clean_up)


def custom_agg_pandas_second_last(df):
	"""
    Custom Pandas aggrgator to return the second last value of a group
    :param df:
    :return:
    """

	if df.shape[0] > 1:
		return df.iloc[-2]
	else:
		return np.nan


def mode_avg_custom(df):
	"""
    Custom pandas function to calculate the mode of a group. if two modes, take the average of the mode
    :param df:
    :return:
    """
	return pd.Series.mode(df).mean()


def mode_max_custom(df):
	"""
    Custom pandas function to calculate the mode of a group. if two modes, take the min of the mode
    :param df:
    :return:
    """
	if len(pd.Series.mode(df)) > 1:
		return pd.Series.mode(df).min()
	else:
		return pd.Series.mode(df).mean()
