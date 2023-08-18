a: ./bin/kudu-tserver --fs_wal_dir `pwd`/cluster/a/wal --rpc_bind_addresses=localhost:7777  --tserver_addresses=localhost:7777,localhost:7778,localhost:7779  --logtostderr=1 --v=$L --vmodule=$GLOG 2>&1
b: ./bin/kudu-tserver --fs_wal_dir `pwd`/cluster/b/wal --rpc_bind_addresses=localhost:7778  --tserver_addresses=localhost:7777,localhost:7778,localhost:7779  --logtostderr=1 --v=$L --vmodule=$GLOG 2>&1
c: ./bin/kudu-tserver --fs_wal_dir `pwd`/cluster/c/wal --rpc_bind_addresses=localhost:7779  --tserver_addresses=localhost:7777,localhost:7778,localhost:7779  --logtostderr=1 --v=$L --vmodule=$GLOG 2>&1

