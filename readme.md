This project serves the article: **Gria: a group-based practical deterministic concurrency control protocol**

the experiment require at least three node for one **coordinator** and two **servers**. The single-node performances (without network overhead) can be get from the log of each server.

#### Server configuration

```shell
vim coroprove/src/t_distributed/coordinator/configure.json
{
    "Host": "172.1.1.1:9999",
    "Server": "Gria", 
    "Fallback": true,
    "Reordering": true, 
    "Thread": 16
}
```

Server including ["**Gria**", "**Aria**", "**Calvin**", "**BOHM**", "**PWV**"], Calvin, BOHM and PWV must set Fallback and Reordering to **false**.

#### Coordinator configuration

```shell
vim coroprove/src/t_distributed/participant/configure.json
{
    "Server": ["172.1.1.1:9999", "172.1.1.2:9999"],
    "Workload": "TPCC", 
    "Warehouse": 256,
    "NewOrderRate": 0.3,
    "WriteRate": 0.5, 
    "Skew": 0.0001,
    "EpochSize": 100
}
```

Workload including ["**TPCC**", "**YCSB**"]. 

#### Run workloads

```shell
// in participant
cd coroprove/src/t_distributed/participant
go run run.go
&{172.1.1.1:9999 Gria true true 16}
Aria ready to perform ...
A client connected :127.0.0.1:64269
raw_cnt:15      waw_cnt:6       cas_cnt:0       batch_cnt:3     commit_cnt:121  reorder_cnt:0   fb_read_cnt:0   fb_commit_cnt:0 fb_abort_cnt:0  fb_block_cnt:0
...

// in coordinator
cd coroprove/src/t_distributed/coordinator
go run run.go
127.0.0.1:64269 : Client connected!
Send epoch 0 with 21127 bytes
2022-04-15 11:58:03.9365032 +0800 CST m=+2.311057601127.0.0.1:64269 Acknowledge
...
```

In participant, the conflict and the block info are shown in the console and the single node performance including breakdown and latency can be gotten in `coroprove/src/t_distributed/participant/participant.log`.

For the coordinator, the communication Infos are shown in the console and the latency can get in `coroprove/src/t_distributed/coordinator/coordinator.log`.
