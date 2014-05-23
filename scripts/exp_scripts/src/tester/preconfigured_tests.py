from remote_setup import TestSetupRemote
from local_setup import TestSetupLocal

__author__ = 'abhigyan'


class RunTestPreConfiguredRemote(TestSetupRemote):

    def test_a_run_given_trace(self):
        """Runs an experiment for which config file specifies all setup parameters"""
        # set these parameters
        self.run_exp_multi_lns()


class RunTestPreConfiguredLocal(TestSetupLocal):

    def test_a_run_given_trace(self):
        """Runs an experiment for which config file specifies all setup parameters"""
        # set these parameters
        self.run_exp_multi_lns()
