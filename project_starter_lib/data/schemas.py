""" Copyright © 2021 by Boston Consulting Group. All rights reserved """
from project_starter_lib.config import config

""" Data schemas for all files associated with the pipeline ingest_source_data"""

# Different dtypes of pandas
# 1. object (str or mixed)
# 2. int32
# 3. float32
# 4. bool
# 5. datetime64[ns]
# 6. category

# INPUT PREFIX INDICATES SCHEMA OF FILES ENTERING THE PIPELINE
# OUTPUT PREFIX INDICATES SCHEMA OF FILES BEING SAVED BY THE PIPELINE

INPUT_INGEST_FILE = {
	'Key'  : 'int16',
	'Value': 'int32',
}

INPUT_AGG_FILE = {
	'Key'  : 'int16',
	'Value': 'int32',
}
