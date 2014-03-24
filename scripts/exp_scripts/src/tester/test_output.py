__author__ = 'abhigyan'


class TestOutput:
    """
    Reads the value of an output variable from text files generated after parsing experiment logs.
    """
    output_folder = ''

    def __init__(self, output_folder):
        self.output_folder = output_folder

    def get(self, output_variable):
        return None





