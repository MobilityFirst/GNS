
swap_file=$1
block_count=$2

dd if=/dev/zero of=$swap_file bs=4K count=$block_count
chmod 600 $swap_file
mkswap $swap_file
echo $swap_file none swap defaults 0 0 | sudo tee -a /etc/fstab
swapon -a 
