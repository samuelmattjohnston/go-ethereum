docker run --rm -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka

./geth init --datadir=/tmp/replicatest ../../genesis.json
./geth init --datadir=/tmp/replicatest2 ../../genesis.json


./geth --kafka.broker=localhost:9092 --mine --miner.etherbase 5409ed021d9299bf6814279a6a1411a7e866a631 --datadir=/tmp/replicatest/ --networkid=19870212 --miner.threads 1 --port 30304 --syncmode full
./geth replica --kafka.broker=localhost:9092 --datadir=/tmp/replicatest2/
