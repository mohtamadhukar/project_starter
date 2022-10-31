""" All configs """
import logging
import os
from datetime import datetime, timedelta

import git
import pandas as pd
import pytz
import yaml
from dotenv import load_dotenv
from envyaml import EnvYAML

from project_starter.data.handlers.s3 import S3StorageHandler

logger = logging.getLogger(__name__)

load_dotenv()

# Also load the config created for M3
config_path = os.path.abspath('config/config.yaml')
cfg = EnvYAML(config_path)

est = pytz.timezone('US/Eastern')
current_time = datetime.now(est)
if cfg['run_configs']['execution_date'] == 'calculated':
	current_date = datetime.now(est).date()
else:
	current_date = pd.to_datetime(cfg['run_configs']['execution_date']).date()

STORAGE_HANDLER = S3StorageHandler()

""" PIPELINE RUN IDS """
PIPELINE_READ_RUN_IDs = cfg['pipeline_read_run_ids']

""" RUN CONFIGS """

ROOT_FOLDER_NAME = cfg['run_configs']['root_folder_name']

""" GIT Related Information """
try:
	GIT_REPO = git.Repo(search_parent_directories=True)
	GIT_BRANCH = GIT_REPO.active_branch.name
	# GIT_HASH = GIT_REPO.head.commit.hexsha
	GIT_HASH = ""
	logger.info(f"Git configs -  git_branch: {GIT_BRANCH}")
except git.exc.InvalidGitRepositoryError as e:
	logger.warning("Not a valid Git repository. Ignoring Git related arguments in logs")
	GIT_REPO = "Not a valid git repo"
	GIT_BRANCH = "Not a valid git repo"
	GIT_HASH = "Not a valid git repo"

""" Which tasks to run in which pipeline """
TASK_RUNNER_INGEST_SOURCE_DATA = cfg['tasks']['ingest_source_data']
TASK_RUNNER_AGG_DATA = cfg['tasks']['agg_data']


""" Input files """

INPUT_FILE = cfg['source_data_paths']['input_file']
