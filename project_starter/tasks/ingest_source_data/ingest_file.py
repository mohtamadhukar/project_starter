""" Task to ingest the new and historical INPUT_SKU Data and save a combined dump """
import logging


from project_starter.common import Task
from project_starter.config import config
from project_starter.data import schemas
from project_starter.data.data_stores import DataStore, checks, clean

logger = logging.getLogger(__name__)


class IngestFile(Task):
	""" """

	def run_task(self):
		logger.info(f"{self.task_name} Task Started")

		df = DataStore(
			file_name=config.INPUT_FILE,
			schema=schemas.INPUT_INGEST_FILE
		).data

		df = clean(
			df=df,
			expected_schema=schemas.INPUT_INGEST_FILE
		)

		checks(df=df, expected_schema=schemas.INPUT_INGEST_FILE)
		self.all_data_stores.ingest_file.data = df

		logger.info(f"{self.task_name} Task Completed")
