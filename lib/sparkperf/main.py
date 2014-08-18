#!/usr/bin/env python

import argparse
import imp
import socket
from subprocess import check_output

from sparkperf.commands import *
from sparkperf.cluster import Cluster
from sparkperf.testsuites import *
from sparkperf.build_spark import SparkBuildManager
from sparkperf.test_plan import TestPlan
from sparkperf.test_runner import TestRunner

import logging
logger = logging.getLogger("sparkperf")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# Spark Versions (if you are not using a pre-built Spark cluster)
# --------------------------------------------------------------------------------------------------
# Commit id used if you are not using an existing Spark cluster.
# The commit ID can specify any of the following:
#     1. A git commit hash         e.g. "4af93ff3"
#     2. A branch name             e.g. "origin/branch-0.7"
#     3. A tag name                e.g. "origin/tag/v0.8.0-incubating"
#     4. A pull request            e.g. "origin/pr/675"
versions = [
    'origin/tag/v1.0.2',
    'origin/branch-1.1',
]

# Repo to download Spark from.
# The remote name in your git repo is assumed to be "origin".
SPARK_GIT_REPO = "https://github.com/apache/spark.git"

# Configuration directory that will be used by each build of Spark:
CLUSTER_SPARK_HOME_DIR = "/root/spark"  # For EC2
SPARK_CONF_DIR = CLUSTER_SPARK_HOME_DIR + "/conf"
SPARK_CONF_DIR = "/Users/joshrosen/Documents/Spark/conf"
SPARK_DRIVER_MEMORY = "20g"


# SPARK_CLUSTER_URL = open("/root/spark-ec2/cluster-url", 'r').readline().strip() # EC2 Cluster
SPARK_CLUSTER_URL = "spark://%s:7077" % socket.gethostname()                  # Local cluster

# Clone a master version of Spark, then copy it to build specific versions / tags.
build_manager = SparkBuildManager(os.path.join(PROJ_DIR, "spark_versions"))
clusters = {}
for version in versions:
    clusters[version] = \
        build_manager.get_cluster(version, SPARK_CLUSTER_URL, SPARK_DRIVER_MEMORY, SPARK_CONF_DIR)

print clusters
exit()

parser = argparse.ArgumentParser(description='Run Spark or Shark peformance tests. Before running, '
    'edit the supplied configuration file.')

parser.add_argument('--config-file', help='override default location of config file, must be a '
    'python file that ends in .py', default="%s/config/config.py" % PROJ_DIR)

args = parser.parse_args()
assert args.config_file.endswith(".py"), "config filename must end with .py"

# Check if the config file exists.
assert os.path.isfile(args.config_file), ("Please create a config file called %s (you probably "
    "just want to copy and then modify %s/config/config.py.template)" %
    (args.config_file, PROJ_DIR))

print "Detected project directory: %s" % PROJ_DIR
# Import the configuration settings from the config file.
print "Loading configuration from %s" % args.config_file
with open(args.config_file) as cf:
    config = imp.load_source("config", "", cf)

plan = config.plan
from pprint import pprint
print len(plan.tests)
pprint(plan.tests)
runner = TestRunner()

if config.USE_CLUSTER_SPARK:
    cluster = Cluster(spark_home=config.SPARK_HOME_DIR, url=config.SPARK_CLUSTER_URL,
                      driver_memory=config.SPARK_DRIVER_MEMORY, spark_conf_dir=config.SPARK_CONF_DIR)
else:
    cluster = Cluster(spark_home=config.SPARK_HOME_DIR, url=config.SPARK_CLUSTER_URL,
                      driver_memory=config.SPARK_DRIVER_MEMORY, spark_conf_dir=config.SPARK_CONF_DIR)
runner.run(plan, config, cluster)


print("All tests have finished running. Stopping Spark standalone cluster ...")
cluster.stop()

print("Finished running all tests.")