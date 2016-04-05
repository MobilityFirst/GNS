sudo mkfs /dev/sdb
sudo mkdir -p /media/ephemeral0
sudo mount -t ext2 /dev/sdb /media/ephemeral0/
sudo chmod 777 /media/ephemeral0/

sudo mkfs /dev/sdd
sudo mkdir -p /media/ephemeral1
sudo mount -t ext2 /dev/sdd /media/ephemeral1/
sudo chmod 777 /media/ephemeral1/
