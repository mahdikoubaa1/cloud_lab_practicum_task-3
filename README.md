# Task 3 - Replicated key-value store (KVS)

In this task, you will extend the single-node key-value store (KVS) and replicate the KVS to multiple servers. This will allow us to have a fault-tolerant KVS that continues to operate even if some servers crash or the network fails.

## The Raft algorithm

To implement the replication in our distributed KVS, we use a **Raft** algorithm. Before you start implementing your solution, first get familiar with this algorithm's main concepts and overall structure by watching the lecture or consulting the extensive resources online.

- Official website (with visualizations) [https://raft.github.io/](https://raft.github.io/)
- Cloud-lab lecture [https://www.youtube.com/watch?v=82zELg4eAtY](https://www.youtube.com/watch?v=82zELg4eAtY)
- Martin Kleppmanns lecture [https://www.youtube.com/watch?v=uXEYuDwm7e4](https://www.youtube.com/watch?v=uXEYuDwm7e4)

## Raft servers

### Leader

The leader handles all client requests (if a client contacts a follower, the 
follower redirects it to the leader) and replicates them on other followers.
It is also responsible for sending out a heartbeat periodically to all followers 
to maintain its leadership. A leader can fail or become disconnected from the 
other servers, in which case a new leader is elected.

The leader can be run using the following command:

```
./kvs-test -a 127.0.0.1:40000 -p 127.0.0.1:41000 -l
```

The leader takes responsibility of the router of task 2.

### Follower

The follower is passive: it issues no requests on its own but respond to 
requests from leaders and candidates. When it has not received heartbeat from 
the leader for a period (election timeout), it transforms to candidate 
and starts the election process.  

## Controller

The controller submits `join`, `get`, `put`, `delete`, `direct_get`, `dropped`, 
`leader` to the API port of the key-value server. Additionally, it submits JOIN_CLUSTER 
requests to nodes to join a cluster of nodes (which sends a JOIN_CLUSTER request to the
routing tier). The code for the controller is provided in the source directory.

The controller can be run like this:

```
./build/ctl-test -a 127.0.0.1:40000 join 127.0.0.1:43000
./build/ctl-test -a 127.0.0.1:40000 put 5 2
./build/ctl-test -a 127.0.0.1:40000 get 5
./build/ctl-test -a 127.0.0.1:40000 del 5
./build/ctl-test -a 127.0.0.1:40000 leader
./build/ctl-test -a 127.0.0.1:40000 dropped
./build/ctl-test -a 127.0.0.1:40000 direct_get 5
```

## Tasks

Your task is to implement the functions that contain the following annotation: 

```TODO (you)```

Additionally, you must include the following messages for the test cases to pass:

- Key and value for a get request, the value should be **ERROR** in case the key doesn't exist
- **OK** for a successful join request


### Task 3.1 - Heartbeat

Your task is to implement the heartbeat mechanism. The leader sends out empty
`append_entries` requests periodically. The follower should update its election timer upon 
receiving the `append_entries` request. You can implement the heartbeat in `Raft::heartbeat()` 
in `/lib/raft/raft.cc` and `P2PHandler::handle_raft_append_entries()` in `/lib/handler/p2p.cc`. 
You can implement the heartbeat with a period of 500ms. 


### Task 3.2 - Leader election

Your task is to implement the Raft leader election. If the election timeout elapses without receiving AppendEntries RPC from the current leader or granting a vote to candidate, the follower gets converted to Candidate.
The candidate starts the election by sending the request to vote to all servers and vote for itself. If it receives votes from the majority of the servers, it becomes the new leader. You can implement it in the `Raft::perform_election()` in  `/lib/raft/raft.cc` and `P2PHandler::handle_raft_vote()` in `/lib/handler/p2p.cc`.


### Task 3.3 - Replication

Your task is to implement the replication mechanism. Upon receiving controller's requests, the leader replicates the received key-value pair in all of its followers. The leader should only respond to the
client once all followers have finished replication. You can implement this in `P2PHandler::handle_key_operation_leader()` and `P2PHandler::handle_raft_append_entries()` in `/lib/handler/p2p.cc`.


## Build instructions

1. Install all required packages:

   ```
   sudo apt-get install build-essential cmake libgtest-dev librocksdb-dev libprotobuf-dev protobuf-compiler libpthread-stubs0-dev zlib1g-dev libevent-dev
   ```

2. Build the source code:

   ```
   mkdir build && cd build/ && cmake .. && make
   ```

If you use NixOS or have nix-shell, you can simply run `nix-shell` in the current directory. 

## Tests

### Test 3.1

This test checks if the heartbeat for failure detection has been implemented correctly. There are three nodes, one leader and two followers. This test will kill two followers, and the leader should be able to return the address of the follower that has been killed. 

The controller will issue 

```
./build/ctl-test -a 127.0.0.1:40000 dropped
```

to the leader to check which nodes have failed. 
An example is shown below:

```
$ ./build/ctl-test -a 127.0.0.1:40000 dropped
Dropped: 127.0.0.1:45000
Dropped: 127.0.0.1:43000
```

To execute this test independently, run:
```
python3 tests/test_failure_detection.py
```

### Test 3.2

This test checks if the leader election has been implemented correctly. There are five nodes, one leader and four followers. The leader will be killed, and the four followers should be able to agree on a new leader. Then, the new leader is killed again, and the other three should be able to elect a new leader.

Every time the leader is killed, the controller will issue `./build/ctl-test -a 127.0.0.1:40000 leader`
to all remaining nodes to see if they agreed on a leader.

To execute this test independently, run:
```
python3 tests/test_leader_election.py
```

### Test 3.3

This test checks if replication has been implemented correctly. The test starts with three nodes, one leader and two followers. A series of `put` requests is issued to the leader. Then, `direct_get` requests are sent to all three nodes to check if they all have the key-value pairs stored. 

```
./build/ctl-test -a 127.0.0.1:40000 direct_get 5
```

A new node joins the cluster in the second part of the test. A series of `direct_get` are sent to the new node to check the implementation. 
The leader should sync the new node with its key-value pair history. 


To execute this test independently, run the following:
```
python3 tests/test_replication.py
```

## References
* [Raft website](https://raft.github.io/)
* [Raft paper](https://raft.github.io/raft.pdf)
* [C++ implementation of Raft](https://github.com/eBay/NuRaft)
* [Protobufs](https://developers.google.com/protocol-buffers/docs/cpptutorial)
* [RocksDB](http://rocksdb.org/docs/getting-started.html)
