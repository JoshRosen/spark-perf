import os
from sparkperf.commands import run_cmd, SBT_CMD


def build_spark(commit_id, target_dir, conf_dir, merge_commit_into_master=False,
                spark_git_repo="https://github.com/apache/spark.git"):
    """
    Download and build Spark.

    :param commit_id: the version to build.  Can specify any of the following:
        1. A git commit hash         e.g. "4af93ff3"
        2. A branch name             e.g. "origin/branch-0.7"
        3. A tag name                e.g. "origin/tag/v0.8.0-incubating"
        4. A pull request            e.g. "origin/pr/675"
    :param target_dir: the directory to clone Spark into.
    :param conf_dir: an existing Spark configuration directory whose contents will be used to
                     configure the new Spark after it's built.
    :param merge_commit_into_master: if True, this commit_id will be merged into `master`; this can
                                     be useful for testing un-merged pull requests.
    :param spark_git_repo: the repo to clone from.  By default, this is the Spark GitHub mirror.
    """
    # Assumes that the preexisting 'spark' directory is valid.
    if not os.path.isdir(target_dir):
        print("Git cloning Spark from %s" % spark_git_repo)
        run_cmd("git clone %s %s" % (spark_git_repo, target_dir))
        # Allow PRs and tags to be fetched:
        run_cmd(("cd %s; git config --add remote.origin.fetch "
                "'+refs/pull/*/head:refs/remotes/origin/pr/*'") % target_dir)
        run_cmd(("cd %s; git config --add remote.origin.fetch "
                "'+refs/tags/*:refs/remotes/origin/tag/*'") % target_dir)
    old_wd = os.getcwd()
    os.chdir(target_dir)
    try:
        # Fetch updates
        print("Updating Spark repo...")
        run_cmd("git fetch")

        # Build Spark
        print("Cleaning Spark and building branch %s. This may take a while...\n" %
              commit_id)
        run_cmd("git clean -f -d -x")

        if merge_commit_into_master:
            run_cmd("git reset --hard master")
            run_cmd("git merge %s -m ='Merging %s into master.'" %
                    (commit_id, commit_id))
        else:
            run_cmd("git reset --hard %s" % commit_id)

        run_cmd("%s clean assembly/assembly" % SBT_CMD)

        # Copy Spark configuration files to new directory.
        print("Copying all files from %s to %s/conf/" % (conf_dir, target_dir))
        assert os.path.exists("%s/spark-env.sh" % conf_dir), \
            "Could not find required file %s/spark-env.sh" % conf_dir
        assert os.path.exists("%s/slaves" % conf_dir), \
            "Could not find required file %s/slaves" % conf_dir
        run_cmd("cp %s/* %s/conf/" % (conf_dir, target_dir))
    finally:
        os.chdir(old_wd)

