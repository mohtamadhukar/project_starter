Copyright © 2021 by Boston Consulting Group. All rights reserved

# Production Guide

The following document is intended to provide all the inforamtion related to scheduled runs and how to monitor them

- We leverage airflow to run all the pipelines in an automated fashion. 
- The pipeline is scheduled to run at 11 am (EST) every Monday.
- The DAG for the airflow is defined in the repository : 
- The webserver and the scheduler has been set up as daemon processes on the EC2 machine to ensure that it restarts if for some reason the EC2 machine also restarts. [Reference tutorial](https://towardsdatascience.com/how-to-run-apache-airflow-as-daemon-using-linux-systemd-63a1d85f9702)

