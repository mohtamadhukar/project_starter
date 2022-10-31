""" Definition of Pipeline and Task """
import logging
from datetime import datetime

import pytz

from project_starter.data.data_stores import AllDataStores

logger = logging.getLogger(__name__)


class Pipeline:
	""" Parent Pipeline Class to be inherited by specific pipelines"""
	name: str
	current_run_id: str
	all_data_stores: AllDataStores = None

	def __init__(self, name):
		self.name = name

		# Initialize current run id for the pipeline
		est = pytz.timezone('US/Eastern')
		current_time = datetime.now(est)
		current_time_str = current_time.strftime("%Y-%m-%d-%H:%M:%S")
		self.current_run_id = f"{current_time_str}_{self.name}"

	def run_pipeline(self):
		""" Run pipeline """
		raise NotImplementedError()


class Task:
	def __init__(self, task_name: str, pipeline: Pipeline):
		self.task_name = task_name
		self.pipeline = pipeline
		self.all_data_stores: AllDataStores = self.pipeline.all_data_stores

	def run_task(self):
		""" Run Task """
		raise NotImplementedError()
