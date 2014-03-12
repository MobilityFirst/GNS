__author__ = 'abhigyan'



class TestCheckerOutput:
    success = False
    key = None
    experiment_output = None
    expected_value = None

    def __init__(self, success, key=None, experiment_output=None, expected_value=None):
        self.success = success
        self.key = key
        self.experiment_output = experiment_output
        self.expected_value = expected_value

    def get_string(self):
        if self.success:
            return "SUCCESS"
        else:
            return "FAILURE" + self.key + '\t' + str(self.experiment_output) + '\t' + str(self.expected_value)


