import os
from subprocess import Popen, PIPE
import sys
import logging
import json
import pprint

from sparkperf import PROJ_DIR
from sparkperf.commands import run_cmd, SBT_CMD
from sparkperf.utils import OUTPUT_DIVIDER_STRING, append_config_to_file, stats_for_results
from sparkperf.config_utils import test_opt_to_flags, java_opt_to_flags


test_env = os.environ.copy()


logger = logging.getLogger("sparkperf")


class PerfTestSuite(object):

    @classmethod
    def build(cls):
        """
        Performs any compilation needed to prepare this test suite.
        """
        pass

    @classmethod
    def is_built(cls):
        """
        :return: True if this test suite has been built / compiled.
        """
        return True

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        raise NotImplementedError

    @classmethod
    def run_test(cls, config, cluster, test_dict, log_directory):
        """
        Run an individual test from this performance suite.
        """
        logger.info("Running test command: '%s' ..." % test_dict["test-script"])
        stdout_filename = "%s/%s.out" % (log_directory, test_dict["name"])
        stderr_filename = "%s/%s.err" % (log_directory, test_dict["name"])
        output_filename = "%s/test-output" % log_directory

        cluster.ensure_spark_stopped_on_slaves()
        java_flags = java_opt_to_flags(test_dict["java-opts"])
        test_flags = test_opt_to_flags(test_dict["test-opts"])

        append_config_to_file(stdout_filename, java_flags, test_flags)
        append_config_to_file(stderr_filename, java_flags, test_flags)
        test_env["SPARK_SUBMIT_OPTS"] = " ".join(java_flags)
        cmd = cls.get_spark_submit_cmd(cluster, test_dict['test-script'], test_flags,
                                       stdout_filename, stderr_filename)
        logger.info("\nRunning command: %s\n" % cmd)
        process = Popen(cmd, shell=True, env=test_env, stdout=PIPE, bufsize=1)
        # The first line of output should be JSON describing the actual configuration / environment
        # that was used (e.g. the SparkConf, System.properties, etc.)
        actual_test_env_str = process.stdout.readline()
        logger.debug("Got test environment string %s" % actual_test_env_str)
        actual_test_env = json.loads(actual_test_env_str)
        logger.info("Test environment:\n%s" % pprint.pformat(actual_test_env))
        # Each of the subsequent lines should be a JSON dict containing a test result:
        while process.poll() is None:
            result_line = process.stdout.readline()
            if result_line.strip() != "":
                result = json.loads(result_line)
                logger.debug("Got result string %s" % result)
                logger.info("Result:\n%s" % pprint.pformat(result))
        process.wait()

        result_string = cls.process_output(config, test_dict['name'], test_flags,
                                           stdout_filename, stderr_filename)
        logger.info("\nResult: " + result_string)
        if "FAILED" in result_string:
            raise Exception("Test Failed!")
        with open(output_filename, 'wa') as out_file:
            out_file.write(result_string + "\n")

    @classmethod
    def get_spark_submit_cmd(cls, cluster, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        raise NotImplementedError


class JVMPerfTestSuite(PerfTestSuite):
    test_jar_path = "/path/to/test/jar"

    @classmethod
    def run_test(cls, config, cluster, test_dict, log_directory):
        assert os.path.isfile(cls.test_jar_path), "Test jar '%s' not found!" % cls.test_jar_path
        super(JVMPerfTestSuite, cls).run_test(config, cluster, test_dict, log_directory)

    @classmethod
    def is_built(cls):
        return os.path.exists(cls.test_jar_path)

    @classmethod
    def get_spark_submit_cmd(cls, cluster, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        spark_submit = "%s/bin/spark-submit" % cluster.spark_home
        cmd = "%s --class %s --master %s --driver-memory %s %s %s  2>> %s | tee -a %s" % (
            spark_submit, main_class_or_script, cluster.url,
            cluster.driver_memory, cls.test_jar_path, " ".join(opt_list),
            stderr_filename, stdout_filename)
        return cmd


class SparkTests(JVMPerfTestSuite):
    test_jar_path = "%s/spark-tests/target/spark-perf-tests-assembly.jar" % PROJ_DIR

    @classmethod
    def build(cls):
        run_cmd("cd %s/spark-tests; %s clean assembly" % (PROJ_DIR, SBT_CMD))

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        with open(stdout_filename, "r") as stdout_file:
            output = stdout_file.read()
        results_token = "results: "
        if results_token not in output:
            print("Test did not produce expected results. Output was:")
            print(output)
            sys.exit(1)
        result_line = filter(lambda x: results_token in x, output.split("\n"))[0]
        result_list = result_line.replace(results_token, "").split(",")
        err_msg = ("Expecting at least %s results "
                   "but only found %s" % (config.IGNORED_TRIALS + 1, len(result_list)))
        assert len(result_list) > config.IGNORED_TRIALS, err_msg
        result_list = result_list[config.IGNORED_TRIALS:]

        result_string = "%s, %s, " % (short_name, " ".join(opt_list))

        result_string += "%s, %.3f, %s, %s, %s\n" % stats_for_results(result_list)

        sys.stdout.flush()
        return result_string


class StreamingTests(JVMPerfTestSuite):
    test_jar_path = "%s/streaming-tests/target/streaming-perf-tests-assembly.jar" % PROJ_DIR

    @classmethod
    def build(cls):
        run_cmd("cd %s/streaming-tests; %s clean assembly" % (PROJ_DIR, SBT_CMD))

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        lastlines = Popen("tail -5 %s" % stdout_filename, shell=True, stdout=PIPE).stdout.read()
        results_token = "Result: "
        if results_token not in lastlines:
            result = "FAILED"
        else:
            result = filter(lambda x: results_token in x,
                            lastlines.split("\n"))[0].replace(results_token, "")
        result_string = "%s [ %s ] - %s" % (short_name, " ".join(opt_list), result)
        return str(result_string)


class MLlibTests(JVMPerfTestSuite):
    test_jar_path = "%s/mllib-tests/target/mllib-perf-tests-assembly.jar" % PROJ_DIR

    @classmethod
    def build(cls):
        run_cmd("cd %s/mllib-tests; %s clean assembly" % (PROJ_DIR, SBT_CMD))

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        with open(stdout_filename, "r") as stdout_file:
            output = stdout_file.read()
        results_token = "results: "
        result = ""
        if results_token not in output:
            result = "FAILED"
        else:
            result_line = filter(lambda x: results_token in x, output.split("\n"))[0]
            result_list = result_line.replace(results_token, "").split(",")
            err_msg = ("Expecting at least %s results "
                       "but only found %s" % (config.IGNORED_TRIALS + 1, len(result_list)))
            assert len(result_list) > config.IGNORED_TRIALS, err_msg
            result_list = result_list[config.IGNORED_TRIALS:]
            result += "Runtime: %s, %.3f, %s, %s, %s\n" % \
                      stats_for_results([x.split(";")[0] for x in result_list])
            result += "Train Set Metric: %s, %.3f, %s, %s, %s\n" %\
                      stats_for_results([x.split(";")[1] for x in result_list])
            result += "Test Set Metric: %s, %.3f, %s, %s, %s" %\
                      stats_for_results([x.split(";")[2] for x in result_list])

        result_string = "%s, %s\n%s" % (short_name, " ".join(opt_list), result)

        sys.stdout.flush()
        return result_string


class PythonTests(PerfTestSuite):

    @classmethod
    def get_spark_submit_cmd(cls, cluster, main_class_or_script, opt_list, stdout_filename,
                             stderr_filename):
        spark_submit = "%s/bin/spark-submit" % cluster.spark_home
        cmd = "%s --master %s pyspark-tests/%s %s 1>> %s 2>> %s" % (
            spark_submit, cluster.url, main_class_or_script, " ".join(opt_list),
            stdout_filename, stderr_filename)
        return cmd

    @classmethod
    def process_output(cls, config, short_name, opt_list, stdout_filename, stderr_filename):
        with open(stdout_filename, "r") as stdout_file:
            output = stdout_file.read()
        results_token = "results: "
        if results_token not in output:
            print("Test did not produce expected results. Output was:")
            print(output)
            sys.exit(1)
        result_line = filter(lambda x: results_token in x, output.split("\n"))[0]
        result_list = result_line.replace(results_token, "").split(",")
        err_msg = ("Expecting at least %s results "
                   "but only found %s" % (config.IGNORED_TRIALS + 1, len(result_list)))
        assert len(result_list) > config.IGNORED_TRIALS, err_msg
        result_list = result_list[config.IGNORED_TRIALS:]

        result_string = "%s, %s, " % (short_name, " ".join(opt_list))

        result_string += "%s, %.3f, %s, %s, %s\n" % stats_for_results(result_list)

        sys.stdout.flush()
        return result_string