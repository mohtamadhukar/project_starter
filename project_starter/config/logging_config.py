""" Configurations related to logging """

# Initialize current run id for the pipeline

logging_file_path = f"./logs/run_logs.log"
LOGGING_CONFIG = {
	'version'                 : 1,
	'disable_existing_loggers': False,
	'formatters'              : {
		'standard': {
			'format' : "%(asctime)s|%(levelname)s|%(name)s|%(funcName)s:%(lineno)d|%(message)s",
			'datefmt': "%Y-%m-%d %H:%M:%S %Z"
		},
	},
	'handlers'                : {
		'console_handler'   : {
			'level'    : 'INFO',
			'formatter': 'standard',
			'class'    : 'logging.StreamHandler',
			'stream'   : 'ext://sys.stdout',
		},
		'local_file_handler': {
			'class'    : 'logging.FileHandler',
			'level'    : 'INFO',
			'formatter': 'standard',
			'filename' : logging_file_path,
			'mode'     : 'w',
		},
	},
	'loggers'                 : {
		''                                            : {  # root logger
			'handlers': ['console_handler', 'local_file_handler'],
			'level'   : 'DEBUG',
		},
		"azure.core.name.policies.http_logging_policy": {
			"level"   : "CRITICAL",
			"handlers": ['console_handler']
		},
		"git"                                         : {
			"level"   : "CRITICAL",
			"handlers": ['console_handler']
		},
		"urllib3"                                     : {
			"level"   : "CRITICAL",
			"handlers": ['console_handler']
		},
		"boto3"                                       : {
			"level"   : "CRITICAL",
			"handlers": ['console_handler']
		},
		"botocore"                                    : {
			"level"   : "CRITICAL",
			"handlers": ['console_handler']
		},
		"gurobipy"                                    : {
			"level"    : "INFO",
			"handlers" : ['console_handler', 'local_file_handler'],
			"propagate": False
		},
		"joblib"                                      : {
			"level"    : "INFO",
			"handlers" : ['console_handler', 'local_file_handler'],
			"propagate": False
		},
	},
}
