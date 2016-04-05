#!/usr/bin/env python2.7
""" Runs features tests on a local setup
"""

import feature_tests
import unittest

__author__ = 'abhigyan'


# include all modules here
test_modules = [feature_tests.FeatureTestMultiNodeLocal]
                # test_check.Test3NodeLocal]

# this runs tests
x = []
for test1 in test_modules:
    x.append(unittest.TestLoader().loadTestsFromTestCase(test1))

test_suite = unittest.TestSuite(x)

unittest.TextTestRunner(verbosity=2, failfast=True).run(test_suite)
