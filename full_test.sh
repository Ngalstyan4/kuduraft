#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

REPLAY_TIMEOUT=140
RECORD_DURATION=40

echogreen() {
    local GREEN='\033[0;32m'
    local NC='\033[0m' # No Color
    echo -e "${GREEN}$1${NC}"
}

echored() {
    local RED='\033[0;31m'
    local NC='\033[0m' # No Color
    echo -e "${RED}$1${NC}"
}

warn_pause() {
	answer=""
	while [[ $answer != [yY] || $answer != [nN]  ]]
	do
		read -p "Continue? (y/n)" answer
		if [[ $answer == [yY] ]]; then
			echogreen "Continuing"
			return
		elif [[ $answer == [nN] ]]; then
			echored "Aborted by user."
			exit 1
		fi
	done
}

node_names=("a" "b" "c")
node_addrs=("localhost:7777" "localhost:7778" "localhost:7779")
# it is really clunky to join the above into the below in bash. hardcode for now
cluster_addr_string="localhost:7777,localhost:7778,localhost:7779"


echogreen "building kuduraft"
make kudu-tserver -j33
rm -rf cluster
mkdir -p cluster
pushd cluster
	for name in ${node_names[@]}; do
		mkdir -p $name
	done
popd
# kill kudu, in case an old one is running
pkill -9 kudu || true
echogreen "recording"
RRMODE=RECORD GLOG=negotiation.cc=5,negotiation*=5 L=5 goreman start&
sleep $RECORD_DURATION
pkill -9 kudu
sleep 1

echogreen "\n\n\nRecorded a cluster trace for $RECORD_DURATION seconds. Will replay each node separately next"
warn_pause

echogreen "deleting cluster state to fresh-start replay"
pushd cluster
	for name in ${node_names[@]}; do
		rm -rf $name/*
	done
popd

for i in ${!node_addrs[@]}; do
	addr=${node_addrs[$i]}
	name=${node_names[$i]}
	# ideally I want to get to warn_pause once either of the next two statements completes. 
	# now it goes to warn_pause right away so if the user presses 'y' while the first replay is in progress, the second one will start right away
	RRMODE=REPLAY ./bin/kudu-tserver --fs_wal_dir `pwd`/cluster/$name/wal --rpc_bind_addresses=$addr  --tserver_addresses=$cluster_addr_string  --logtostderr=1 || true &
	this_replay_proc=`pgrep kudu`
	sleep $REPLAY_TIMEOUT && kill -9 $this_replay_proc && echored "\n\n KILLED REPLAY. timeout of $REPLAY_TIMEOUT seconds" &
	echo "\n\n"
	warn_pause
done

echogreen "done with replays!"
