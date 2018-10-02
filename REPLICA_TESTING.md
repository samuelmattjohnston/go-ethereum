## Short and sweet read replicacluster
    docker run --rm -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka

    ./geth init --datadir=/tmp/replicatest ../../genesis.json
    ./geth init --datadir=/tmp/replicatest2 ../../genesis.json


    ./geth --kafka.broker=localhost:9092 --mine --miner.etherbase 5409ed021d9299bf6814279a6a1411a7e866a631 --datadir=/tmp/replicatest/ --networkid=19870212 --miner.threads 1 --port 30304 --syncmode full
    ./geth replica --kafka.broker=localhost:9092 --datadir=/tmp/replicatest2/


## Running with writable replica nodes

Check out this code base so it lives at:  
`${HOME}/go/src/github.com/ethereum/go-ethereum/cmd/geth`  

Or look up overriding you `$GOPATH` https://github.com/golang/go/wiki/GOPATH

#### OPTIONAL workaround for apps that infer the chainId from networkId:

If you're not going to use chainId `19870212`, edit the following file: `cmd/geth/replicacmd.go:80`

More details will be provided at bottom


#### Build it

    go build cmd/geth

#### Prepare your genesis.json:

For these steps, lives in the top level directory of this repository  
For version 1.9 and lower of geth, keep in mind your `chainId`  

    {
      "config": {
            "chainId": 19870212,
            "homesteadBlock": 0,
            "eip155Block": 0,
            "eip158Block": 0
        },
      "alloc"      : {},
      "coinbase"   : "0x0000000000000000000000000000000000000000",
      "difficulty" : "0x20000",
      "extraData"  : "",
      "gasLimit"   : "0x2fefd8",
      "nonce"      : "0x0000000000000042",
      "mixhash"    : "0x0000000000000000000000000000000000000000000000000000000000000000",
      "parentHash" : "0x0000000000000000000000000000000000000000000000000000000000000000",
      "timestamp"  : "0x00"
    }

#### Seed the disk directories of the master and slave replica

For this guide, we'll make the:  
- master node use /tmp/replicatest  
- slave replica use /tmp/replicatest2  

eg:

    ./geth init --datadir=/tmp/replicatest ../../genesis.json
    ./geth init --datadir=/tmp/replicatest2 ../../genesis.json


#### Stand up a kafka server

    docker run --rm -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka

#### Run it


Set the master flags up so:  
- `networkid` matches your `chainId` in the `genesis.json`  
- specify the `genesis.json` you created earlier  
- specify the data directory we seeded earlier  
- use the flags: `--rpc --rpccorsdomain='*'` if doing testing on only the master node, won't do anything if we are enabling replicas  

eg:

    ./geth --kafka.broker=localhost:9092 --mine --miner.etherbase 1c522b369b0e5981a50687e97e442754538e3dfd --datadir=/tmp/replicatest/ --networkid=19870212 --miner.threads 1 --port 30304 --syncmode full --gcmode archive

Set up the slave replica flags so:  
- `kafka.tx.topic` is set to something, `txtest` is this example, otherwise you'll have read only replicas  
- specify the data directory we seeded earlier  

eg:

    ./geth replica --kafka.tx.topic=txtest --kafka.broker=localhost:9092 --datadir=/tmp/replicatest2/


#### Follow your kafka logs from the kafka docker container

    docker exec -it {id} /bin/bash
    ./opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic txtest --from-beginning


#### Try to submit an transaction

Generate coins:  
Generate your account, and then restart your master, changing the flag for `miner.etherbase`  

Your transaction will be displayed on your kafaka server, and you should see it git spit out on the console when you run the command provided earlier.

*NOTE* You need to make sure you are running the **BETA** of Metamask if testing with it, so the chainId resolution will work properly. (Occurred to me when I was only running the master, this was the fix I had to do)

#### Feed the transaction back into the master

Write an app, or do it by hand for testing

doing it by hand:

    geth attach /tmp/replicatest/geth.ipc  
    web3.personal.newAccount()  

Take that account, and then restart your master, changing the flag for `miner.etherbase` ( Yes you do need coin for this )

Post your transaction to the master (Should have been obtained from the kafaka queue. Posted earlier manually):

    geth attach /tmp/replicatest/geth.ipc
    web3.eth.defaultAccount = web3.eth.accounts[0]
    personal.unlockAccount(web3.eth.defaultAccount)
    web3.eth.sendTransaction({transaction})


# ISSUES ENCOUNTERED:
## Invalid Sender

Your client is using the wrong `chainId` due to improperly configured `networkId`. Make sure they are the same. Master defaults to `1` if not set, and the read replcias are hard coded.  
This is due to a long outstanding issue since geth 1.6 and apps that infer the `chainId` , I'll post some reading material:  

https://github.com/MetaMask/metamask-extension/issues/1722  
MetaMask can not send transactions to localhost:8545 · Issue #1722 · MetaMask/metamask-extension  
> i think geth 1.6 no longer defaults the net id to chain id, so we're hearing this issue recently. we dont currently have a way to query the chainId directly so have always used the network id

https://github.com/ethereum/EIPs/blob/master/EIPS/eip-155.md  
ethereum/EIPs The Ethereum Improvement Proposal repository.  


https://github.com/ethereumproject/go-ethereum/wiki/FAQ#what-is-the-difference-between-chain-id-chain-identity-and-network-id  
ethereumproject/go-ethereum FAQ  

> Flags For Your Private Network
> There are some command line options (also called "flags") that are necessary in order to make sure that your network is private.
> --nodiscover
Use this to make sure that your node is not discoverable by people who do not manually add you. Otherwise, there is a chance that your node may be inadvertently added to a stranger's > node if they have the same genesis file and network id.

**FIXED IN** https://github.com/ethereum/go-ethereum/pull/17617

## Error: invalid address

This was encountered in geth command line shell

Create and unlock a default account, as described in manually posting to your master node transactions

## Error: insufficient funds for gas * price + value

This was encountered in geth command line shell

Your unlocked default account needs funds to send transactions. I don't know why, but some reason this happened to me. mine on it for a sec by restarting master, and changing the mining address, as described above
