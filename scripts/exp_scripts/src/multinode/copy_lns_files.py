#!/usr/bin/env python


def main():
    f = open('pl_lns')
    i = 100
    fw = open('cmd.sh', 'w')
    for line in f:
        host = line.strip()
        cmd = 'ssh -i auspice.pem ec2-user@' + host + ' "wget https://s3.amazonaws.com/update100m/update_' +str(i) +  '"'
        fw.write(cmd)
        fw.write('\n')
        cmd = 'ssh -i auspice.pem ec2-user@' + host + ' "wget https://s3.amazonaws.com/lookup100m/lookup_' +str(i) +  '"'
        fw.write(cmd)
        fw.write('\n')
        i += 1
    fw.close()
    

if __name__ == "__main__":
    main()

