#!/usr/bin/env python

import argparse
import imp
import time

from sparkperf.commands import *
from sparkperf.cluster import Cluster
from sparkperf.testsuites import *
from sparkperf.build_spark import build_spark
from sparkperf.test_plan import TestPlan
from sparkperf.test_runner import TestRunner


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