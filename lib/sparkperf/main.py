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


def main():
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser(
        description='Run Spark or Shark peformance tests. Before running, '
        'edit the supplied configuration file.')

    parser.add_argument('--config-file', help='override default location of config file, must be a '
                                              'python file that ends in .py',
                        default="%s/config/config.py" % PROJ_DIR)

    args = parser.parse_args()
    assert args.config_file.endswith(".py"), "config filename must end with .py"

    # Check if the config file exists.
    err_msg = ("Please create a config file called %s (you probably "
               "just want to copy and then modify %s/config/config.py.template)" %
               (args.config_file, PROJ_DIR))
    assert os.path.isfile(args.config_file), err_msg
    logger.info("Detected project directory: %s" % PROJ_DIR)
    # Import the configuration settings from the config file.
    logger.info("Loading configuration from %s" % args.config_file)
    with open(args.config_file) as cf:
        config = imp.load_source("config", "", cf)
    run_with_config(config)


def run_with_config(config):
    # Configure Spark versions
    built_in_cluster = Cluster(config.SPARK_HOME_DIR, config.SPARK_CLUSTER_URL,
                               config.SPARK_DRIVER_MEMORY, config.SPARK_CONF_DIR)
    if config.USE_CLUSTER_SPARK:
        clusters = {
            'default': built_in_cluster
        }
    else:
        build_manager = SparkBuildManager(os.path.join(PROJ_DIR, "spark_versions"))
        clusters = {}
        for version in config.SPARK_COMMIT_IDS:
            clusters[version] = \
                build_manager.get_cluster(version, config.SPARK_CLUSTER_URL,
                                          config.SPARK_DRIVER_MEMORY, config.SPARK_CONF_DIR)

    try:
        logger.info("Attempting to stop the built-in Spark cluster")
        # Try to stop the built-in Spark cluster
        built_in_cluster.stop()
        built_in_cluster.ensure_spark_stopped_on_slaves()
    except:
        logger.error("Error while stopping the built-in Spark cluster")

    plan = config.plan
    logger.info("Loaded test plan with %i tests" % len(plan.tests))
    runner = TestRunner()

    for (version, cluster) in clusters.items():
        logger.info("Running tests against Spark version %s" % version)
        logger.debug("Cluster SPARK_HOME is %s" % cluster.spark_home)
        cluster.ensure_spark_stopped_on_slaves()
        try:
            cluster.start()
            runner.run(plan, config, cluster, config.SPARK_OUTPUT_FILENAME)
        finally:
            logger.info("Finished running tests; stopping cluster")
            cluster.stop()
    logger.info("All tests have finished running.")


if __name__ == '__main__':
    main()