import itertools
from sparkperf.config_utils import OptionSet


class TestPlan(object):
    """
    A plan that describes a set of tests to run.
    """

    def __init__(self):
        self.tests = []

    def add_test(self, name, test_suite, test_script, java_opts, test_opts, scale_factor=1.0):
        if not isinstance(java_opts, dict):
            java_opts = dict(itertools.chain.from_iterable(d.iteritems() for d in java_opts))
        if not isinstance(test_opts, dict):
            test_opts = dict(itertools.chain.from_iterable(d.iteritems() for d in test_opts))
        def opt_to_array(kv):
            (k, v) = kv
            if isinstance(v, OptionSet):
                return [(k, u) for u in v.to_array(scale_factor)]
            else:
                return [(k, v)]
        java_opt_arrays = [opt_to_array(opt) for opt in java_opts.iteritems()]
        test_opt_arrays = [opt_to_array(opt) for opt in test_opts.iteritems()]
        def enumerate_options():
            for java_opt_list in itertools.product(*java_opt_arrays):
                j_dict = dict(java_opt_list)
                for test_opt_list in itertools.product(*test_opt_arrays):
                    t_dict = dict(test_opt_list)
                    yield {
                        'name': name,
                        'test_suite': test_suite,
                        'test_script': test_script,
                        'java_opts': j_dict,
                        'test_opts': t_dict,
                    }
        new_tests = list(enumerate_options())
        self.tests.extend(new_tests)


    def add_from_config(self, test_suite, tests_to_run):
        """
        :param test_suite:  The suite to run.
        :param tests_to_run:  A list of 5-tuple elements specifying the tests to run.  The elements
            are (short_name, main_class_or_script, scale_factor, java_opt_sets, opt_sets).  See the
            'Test Setup' section in config.py.template for more info.
        :return:
        """
        # Enumerate all combinations of the OptionSets
        for short_name, main_class_or_script, scale_factor, java_opt_sets, opt_sets in tests_to_run:
            java_opt_set_arrays = [i.to_array(scale_factor) for i in java_opt_sets]
            opt_set_arrays = [i.to_array(scale_factor) for i in opt_sets]

            def enumerate_options():
                for java_opt_list in itertools.product(*java_opt_set_arrays):
                    for opt_list in itertools.product(*opt_set_arrays):
                        yield {
                            "short_name": short_name,
                            "test_suite": test_suite,
                            "main_class_or_script": main_class_or_script,
                            "opt_list": opt_list,
                        }
            self.tests += list(enumerate_options())
