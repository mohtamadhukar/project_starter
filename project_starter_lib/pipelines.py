""" All Pipeline Definitions"""

import logging
import gc

from project_starter_lib.config import config
from project_starter_lib import constants
from project_starter_lib.common import Pipeline
from project_starter_lib.constants import (
	TASK_INGEST_FILE,
	TASK_AGG_FILE
	)
from project_starter_lib.tasks.ingest_source_data.ingest_file import IngestFile
from project_starter_lib.tasks.agg_data.agg_file import AggFile
from project_starter_lib.utils import decorate_run_pipeline

logger = logging.getLogger(__name__)


class IngestSourceData(Pipeline):
	""" Ingest Source Data Pipeline"""

	@decorate_run_pipeline
	def run_pipeline(self):
		""" Specify and run all tasks of the name"""

		if config.TASK_RUNNER_INGEST_SOURCE_DATA[constants.TASK_INGEST_FILE]:
			IngestFile(task_name=constants.TASK_INGEST_FILE, pipeline=self).run_task()



class AggData(Pipeline):
	""" Aggregate up TXN and Inv Data and calculate sell by date for all stores """

	@decorate_run_pipeline
	def run_pipeline(self):
		""" """

		if config.TASK_RUNNER_AGG_DATA[constants.TASK_AGG_FILE]:
			AggFile(task_name=constants.TASK_AGG_FILE, pipeline=self).run_task()

