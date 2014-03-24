__author__ = 'abhigyan'


from test import Test
from test_checker_output import TestCheckerOutput
from test_runner import test_runner, run_all_tests


def run_single_node_tests():
    all_tests = [Test("resources/singlenode/test1_config", test_runner, test1_checker),
                 Test("resources/singlenode/test2_config", test_runner, test2_checker),
                 Test("resources/singlenode/test3_config", test_runner, test3_checker)]

    run_all_tests(all_tests)


def test1_checker(test_output):
    return TestCheckerOutput(True)


def test2_checker(test_output):
    return TestCheckerOutput(True)


def test3_checker(test_output):
    return TestCheckerOutput(True)
