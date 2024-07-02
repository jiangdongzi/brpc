cur=$(pwd)
cd /home/jdz201503_wsl/github/brpc/build
make -j
cd $cur
make -j
./memcache_client -server=mongodb://myUser:password123@localhost:7017/myDatabase
