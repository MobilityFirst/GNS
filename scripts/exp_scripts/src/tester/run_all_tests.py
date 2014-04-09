#!/usr/bin/env python2.7
import test_check

__author__ = 'abhigyan'


import unittest

# include all modules here
test_modules = [test_check.Test1NodeLocal,
                test_check.Test3NodeLocal]

# this runs tests
x = []
for test1 in test_modules:
    x.append(unittest.TestLoader().loadTestsFromTestCase(test1))

test_suite = unittest.TestSuite(x)

unittest.TextTestRunner(verbosity=2).run(test_suite)
