""" Task to Prepare Sell By Date File """
import logging
from datetime import timedelta

import numpy as np
import pandas as pd

from project_starter.common import Task
from project_starter.config import config
from project_starter.data import schemas
from project_starter.data.data_stores import DataStore, clean

logger = logging.getLogger(__name__)


class AggFile(Task):
	""" """

	def run_task(self):
		logger.info(f"{self.task_name} Task Started")

		df_input_file = self.all_data_stores.ingest_file.data

		df_agg = df_input_file.groupby('Key',as_index=False).agg({'Value':'sum'})

		self.all_data_stores.aggregated_file.data = df_agg

		logger.info(f"{self.task_name} Task Completed")
