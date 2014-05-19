import os

__author__ = 'abhigyan'


def run_cmds(cmds):
    """ Runs a list of commands usng 'parallel'"""

    cmd_file = '/tmp/cmds.sh'
    fw = open(cmd_file, 'w')
    for cmd in cmds:
        fw.write(cmd)
        if cmd[-1] != '\n':
            fw.write('\n')
    fw.close()
    # run 30 jobs in parallel
    os.system('parallel -j 50 -a ' + cmd_file)
    os.system('rm ' + cmd_file)
