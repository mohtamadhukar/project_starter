""" Copyright © 2021 by Boston Consulting Group. All rights reserved """

import click

""" Entry point for the repo"""
import logging
from collections import OrderedDict

from project_starter_lib.config import config
from project_starter_lib.constants import (
	PIPELINE_INGEST_SOURCE_DATA,
	PIPELINE_AGG_DATA,
)
from project_starter_lib.data.data_stores import AllDataStores, DataStore
from project_starter_lib.pipelines import (
	IngestSourceData,
	AggData,
)

# Configure logger for use in package
logger = logging.getLogger(__name__)

cmd_line_pipeline_choices = [
	'all',
	PIPELINE_INGEST_SOURCE_DATA,
	PIPELINE_AGG_DATA,
]


@click.command()
@click.option(
	"--pipelines", '-p',
	default=["all"],
	type=click.Choice(cmd_line_pipeline_choices, case_sensitive=False),
	multiple=True,
	show_default=True  # This is provided to show default values in help
)
def cli(pipelines):
	"""
    Run pipelines
    :param pipelines: pipelines which we need to run. Passes using command line
    :return:
    """

	logger.info(f"Run Configs - root_folder_name:{config.ROOT_FOLDER_NAME}")

	# Specify in which order the pipelines should run
	pipeline_class_map = OrderedDict({
		PIPELINE_INGEST_SOURCE_DATA      : IngestSourceData,
		PIPELINE_AGG_DATA                : AggData,
	})

	# By default run all pipelines
	if len(pipelines) == 0:
		pipelines = {'all'}

	all_pipeline_names = pipeline_class_map.keys()

	# check to see if any unknown pipelines have been specified
	unknown_pipelines = (set(pipelines) -
						 set(pipeline_class_map.keys()) -
						 {'all'})

	# By default run all pipelines
	if unknown_pipelines:
		logger.error(f'Found unknown pipelines {unknown_pipelines} in command line arguments {pipelines}')
		return
	elif 'all' in pipelines:
		logger.debug(f'Found "all" in pipeline arguments, expanding to {all_pipeline_names}')
		filtered_pipeline_names = all_pipeline_names
	else:
		# filter down to each specified name just once, and in standard execution order
		filtered_pipeline_names = [p for p in all_pipeline_names if p in pipelines]

	pipeline_current_run_ids = {}

	# Create run ids for all the pipelines and objects of each pipeline so that we can run the pipelines respectively
	for pipeline_name in filtered_pipeline_names:
		pipeline_object = pipeline_class_map[pipeline_name](pipeline_name)
		# Get the current run id of the pipeline that was created by the Pipeline class
		pipeline_current_run_ids[pipeline_name] = pipeline_object.current_run_id
		logger.info(f"Current Run ID for {pipeline_name} : {pipeline_object.current_run_id} ")

		# Replace the instance of the pipeline in the pipeline_class_map dictionary so that the dict has the
		# latest pipeline names
		pipeline_class_map[pipeline_name] = pipeline_object

	# Create a data_store object which can be added to all the pipelines
	all_data_store = AllDataStores(pipeline_current_run_ids)

	# Add data_store object to all the pipeline objects
	for pipeline_name in all_pipeline_names:
		pipeline_class_map[pipeline_name].all_data_stores = all_data_store

	logger.info(f"Started running the pipelines {filtered_pipeline_names}")
	# Run the pipelines that we want to run
	for pipeline_name in filtered_pipeline_names:
		pipeline_object = pipeline_class_map[pipeline_name]
		pipeline_object.run_pipeline()

	logger.info("Completed Running all pipelines")


if __name__ == "__main__":
	exit_code = 1
	try:
		cli()
	except Exception:
		logger.error('Got exception on main runner', exc_info=True)  # Save all logs to S3
		exit_code = -1
	finally:
		all_logs = open('./logs/run_logs.log', 'rb').read()
		time_stamp = config.current_time.strftime(format='%Y-%m-%d-%H%M%S')
		DataStore(
			file_name=f"output_data/{config.ROOT_FOLDER_NAME}/logs/{time_stamp}_run_logs.txt",
			flag_copy_to_latest=False
		).data = all_logs
	exit(exit_code)
