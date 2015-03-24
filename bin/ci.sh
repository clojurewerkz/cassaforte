git clone https://github.com/pcmanus/ccm.git /tmp/ccm
cd /tmp/ccm
sudo ./setup.py install
mkdir -p /tmp/cassandra-data/test
ccm create test -n 1 -s -i 127.0.0. -b -v 2.1.3 --config-dir=/tmp/cassandra-data
