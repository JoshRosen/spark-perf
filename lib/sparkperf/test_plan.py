import itertools
from sparkperf.config_utils import OptionSet


class TestPlan(object):
    """
    A plan that describes a set of tests to run.  A TestPlan is passed to a TestRunner in order
    to execute it on an actual cluster.
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
                        'test-suite': test_suite,
                        'test-script': test_script,
                        'java-opts': j_dict,
                        'test-opts': t_dict,
                    }
        new_tests = list(enumerate_options())
        self.tests.extend(new_tests)