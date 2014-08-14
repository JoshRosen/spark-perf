import time


class TestRunner(object):
    def __init__(self):
        pass

    def run(self, plan, cluster):
        # A cluster might be running from an earlier test, so try shutting it down:
        cluster.stop()
        # Ensure all shutdowns have completed (no executors are running).
        cluster.ensure_spark_stopped_on_slaves()
        # Allow some extra time for slaves to fully terminate.
        time.sleep(5)

        # Start the cluster
        cluster.sync_spark()
        cluster.start()
        time.sleep(5) # Starting the cluster takes a little time so give it a second.

        # Run the tests
        print "About to run %i tests" % len(plan.tests)
        for test in plan.tests: