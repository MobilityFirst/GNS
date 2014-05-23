

""" Uses a model to predict time-to-connect as a function of lookup latency, update rate, update latency
and TCP timeout value. """

__author__ = 'abhigyan'


def print_time_to_connects():
    lookup = 45.0  # ms
    timeout = 5000.0  # ms
    update_latency = 300 # ms
    num_updates = 1
    while num_updates <= 100000:
        update_rate = num_updates/86400000.0
        ttc = get_avg_time_to_connect(lookup, timeout, update_rate, update_latency)
        num_updates *= 10
        print 'NumUpdate\t', num_updates, '\tTTC\t', ttc


def get_avg_time_to_connect(lookup, timeout, update_rate, update_latency):
    import math
    p = math.exp(- update_rate * update_latency)
    c = lookup + (1 - p)*(lookup + timeout)/p
    return c


if __name__ == '__main__':
    print_time_to_connects()