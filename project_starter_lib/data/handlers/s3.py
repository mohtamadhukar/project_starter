""" Class whose object is used to interact with S3 Bucket"""
import json
import logging
import os
import pickle
from typing import List

import boto3
import boto3.session
import pandas as pd
from dotenv import load_dotenv

from project_starter_lib.data.handlers.common import StorageHandler

logger = logging.getLogger(__name__)

load_dotenv()

S3_BUCKET = os.getenv("AWS_BUCKET_NAME")


class S3StorageHandler(StorageHandler):
	""" """

	def get_s3_file_path(self, file_path):
		"""Returns the s3 file path, in form s3://{bucket_name}/{path}

		Parameters
		----------
		Returns
		-------
		s3_file_path : str
			S3 file path
		"""
		# Get the bucket_name
		s3_bucket_name = S3_BUCKET

		# Form path
		s3_file_path = "s3://" + s3_bucket_name + "/" + file_path

		return s3_file_path

	def confirm_access(self):
		""" raise if bad aws credentials, bucket doesn't exist, or forbidden from bucket """
		s3 = boto3.client("s3")
		s3.head_bucket(Bucket=S3_BUCKET)

	def load(self, path, **kwargs):
		"""Download data from S3 bucket and return a pandas dataframe.
		Parameters
		----------
		self.path : str
			S3 file path of file
		Returns
		-------
		data
			Data object output
		"""

		logger.info(f"Reading file from {path} with filters {kwargs.get('filters')}")
		if path.endswith(".pkl"):
			# Here we create a new session per function call
			session = boto3.session.Session()
			s3_resource = session.resource("s3")
			response = s3_resource.Object(S3_BUCKET, path).get()["Body"].read()
			data = pickle.loads(response)

		elif path.endswith(".txt"):
			# Here we create a new session per function call
			session = boto3.session.Session()
			s3_resource = session.resource("s3")
			data = s3_resource.Object(S3_BUCKET, path).get()["Body"].read()

		elif path.endswith(".xlsx"):
			data = pd.read_excel(self.get_s3_file_path(path), **kwargs)
		# Had to add a .csv. or condition because of the file name being uploaded to s3 :(
		elif path.endswith(".csv") or path.endswith(".csv."):
			parser = lambda x: pd.to_datetime(x, errors='coerce', infer_datetime_format=True)
			data = pd.read_csv(
				self.get_s3_file_path(path),
				date_parser=parser,
				**kwargs

			)
		elif path.endswith(".parquet.gzip") or path.endswith(".parquet"):
			data = pd.read_parquet(
				self.get_s3_file_path(path),
				filters=kwargs.get('filters'),
				columns=kwargs.get('columns')
			)
		else:
			raise Exception("Not implemented data download: " + path)

		return data

	def save(self, file_path, data, **kwargs):
		"""Saves data as pkl, csv or json to S3 bucket. If csv, then
		should be a pandas dataframe
		Parameters
		----------
		data : Object
		file_path : str
			S3 file path. It should end with .pkl, .csv or .json
		**kwargs : Optional params
			Optional csv / json parameters
		"""

		if file_path.endswith(".pkl"):
			bucket = S3_BUCKET
			pickle_byte_obj = pickle.dumps(data)
			# Here we create a new session per function call
			session = boto3.session.Session()
			s3_resource = session.resource("s3")
			file_path = file_path
			logger.debug("Saving pkl to " + file_path)
			s3_resource.Object(bucket, file_path).put(Body=pickle_byte_obj)
		elif file_path.endswith(".csv"):
			s3_path = self.get_s3_file_path(file_path)
			logger.debug("Saving csv to " + s3_path)
			data.to_csv(s3_path, index=False, **kwargs)
		elif file_path.endswith(".xlsx"):
			s3_path = self.get_s3_file_path(file_path)
			logger.debug("Saving Excel to " + s3_path)
			data.to_excel(s3_path, index=False, **kwargs)
		elif file_path.endswith(".json"):
			bucket = S3_BUCKET
			json_bytes = json.dumps(data, **kwargs).encode()
			logger.debug("Saving json to " + file_path)
			s3_resource = boto3.resource("s3")
			s3_resource.Object(bucket, file_path).put(Body=json_bytes)
		elif file_path.endswith(".parquet.gzip") or file_path.endswith(".parquet"):
			s3_path = self.get_s3_file_path(file_path)
			logger.debug("Saving parquet.gzip to " + s3_path)
			data.to_parquet(
				s3_path,
				index=False,
				partition_cols=kwargs.get('partition_cols')
			)
		elif file_path.endswith(".txt") or file_path.endswith(".log"):
			s3 = boto3.resource('s3')
			s3_object = s3.Object(S3_BUCKET, file_path)
			s3_object.put(Body=data)
		else:
			raise NotImplementedError()

	def list_files(self, prefix: str = "", suffix: str = "") -> List[str]:
		"""Retrieve the paths of the contents in the bucket.
		This includes both files and folders.
		Parameters
		----------
		prefix : str
			Only fetch keys that start with this prefix (optional).
		suffix : str
			Only fetch keys that end with this suffix (optional).
		Returns
		-------
		matches : list of str
			Files that match the given prefix and/or suffix
		"""
		kwargs = {
			"Bucket": S3_BUCKET,
			"Prefix": prefix
		}
		s3 = boto3.client("s3")

		matches = []
		while True:
			resp = s3.list_objects_v2(**kwargs)
			for obj in resp["Contents"]:
				key = obj["Key"]
				if key.endswith(suffix):
					matches.append(key)
			try:
				kwargs["ContinuationToken"] = resp["NextContinuationToken"]
			except KeyError:
				break

		return matches

	def copy(self, source_location, dest_location, **kwargs):
		"""
		Copy an object from one S3 location to another
		:param source_location:
		:param dest_location:
		:return:
		"""

		logger.info(f"Copying {source_location} to {dest_location} folder")

		s3 = boto3.resource('s3')

		# Deleting data at dest location if it exists
		bucket = s3.Bucket(S3_BUCKET)
		objects = bucket.objects.filter(Prefix=dest_location)
		objects.delete()

		copy_source = {
			'Bucket': S3_BUCKET,
			'Key'   : source_location
		}
		s3.meta.client.copy(copy_source, S3_BUCKET, dest_location)

	def upload(self, local_path, dest_path, **kwqrgs):
		""" Upload a file from local to S3"""

		s3 = boto3.resource('s3')
		file_name = dest_path.split('/')[-1]
		for subdir, dirs, files in os.walk(local_path):
			for file in files:
				full_local_path = os.path.join(subdir, file)
				dest_full_path = f"{dest_path}{full_local_path.split(file_name)[-1]}"
				logger.info(f"Uploading file from {full_local_path} to {dest_full_path} ")
				s3.meta.client.upload_file(full_local_path, S3_BUCKET, dest_full_path)

	def delete(self, path: str, **kwargs):
		""" Deleting a folder """
		logger.info(f"Deleting Data at {path}")

		s3 = boto3.resource('s3')

		# Deleting data at dest location if it exists
		bucket = s3.Bucket(S3_BUCKET)
		objects = bucket.objects.filter(Prefix=path)
		objects.delete()

	def download(self, to_dir, from_path, file_name, **kwargs):
		"""

		:param file_name:
		:param to_dir:
		:param from_path:
		:return:
		"""

		s3_client = boto3.client('s3')
		if from_path.endswith(file_name):
			full_path = os.path.join(to_dir, file_name)
		else:
			path_suffix = from_path.split(file_name)[-1][1:]
			full_path = os.path.join(to_dir, file_name, path_suffix)

		# Ensure directory is present
		to_file_name = full_path.split(os.sep)[-1]
		to_full_directory = full_path.replace(to_file_name, "")
		if not os.path.isdir(to_full_directory) and not os.path.exists(to_full_directory):
			try:
				os.makedirs(to_full_directory)
			except FileExistsError:
				logger.info("Directory exists")

		logger.info(f"Copying file from {from_path} to {full_path}")
		s3_client.download_file(S3_BUCKET, from_path, full_path)
