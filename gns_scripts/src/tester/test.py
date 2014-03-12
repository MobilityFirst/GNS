__author__ = 'abhigyan'


# class defines a test object.
class Test:

    config_file = ''
    test_checker = None
    test_runner = None
    test_result = False

    def __init__(self, config_file, test_runner, test_checker):
        self.config_file = config_file;
        self.test_checker = test_checker
        self.test_runner = test_runner

    def run(self):
        test_output = self.test_runner(self.config_file)
        self.test_result = self.test_checker(test_output)
        return self.test_result

