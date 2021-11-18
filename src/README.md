# Build
1. Clone the copilot repository
```shell
git clone git@github.com:princeton-sns/copilot.git
```
2. Build
```shell
cd copilot
make
```
# Run
1. Start the master service that waits for 3 replicas to connect 
```shell
(./bin/master -N 3 -twoLeaders=true) &
```
2. Start 3 replicas that run Copilot protocol.
```shell
(./bin/server -maddr=<master-ip> -mport=7087 -addr=<node-0-ip> -port=7070 -copilot=true -exec=true -dreply=true) &
(./bin/server -maddr=<master-ip> -mport=7087 -addr=<node-1-ip> -port=7070 -copilot=true -exec=true -dreply=true) &
(./bin/server -maddr=<master-ip> -mport=7087 -addr=<node-2-ip> -port=7070 -copilot=true -exec=true -dreply=true) &
```
These replicas will connect to the master and wait for client connections.
(Note: we assume the replicas are on separate machines.
If they are on the same machine, each replica should use a different port number `-port`.
To run Latent Copilot, replace `-copilot=true` with `-latentcopilot=true`.)

3. Start a Copilot closed-loop client with an `id = 0`
```shell
(./bin/clientmain -maddr=<master-ip> -mport=7087 -twoLeaders=true -id=0) &
```

For more options to specify for a server/client, please refer to [`startexpt.sh`](https://github.com/princeton-sns/slowdown/blob/b1ff691c9935134134b5a638f095bbf3db7d9648/startexpt.sh)
# Running Copilot on Emulab
## System Requirements
We evaluated Copilot on the [Emulab testbed](https://www.emulab.net/) using the [d430 machines](https://wiki.emulab.net/wiki/d430),
each of which has one 2.4GHz 64-bit 8-Core processor, 64GB RAM, and is networked with 1 Gbps Ethernet.
Each machine ran `Ubuntu 16.04 STD`.
We built Copilot using a Go version `go1.15.2 linux/amd64`.
We used a Python version `2.7.12`.

## Running `startexpt.sh`
On Emulab, after an experiment is created and swapped in, each node is assigned with an alias `node-X` (`X >= 0`).
We provide an experiment script [`startexpt.sh`](https://github.com/princeton-sns/slowdown/blob/b1ff691c9935134134b5a638f095bbf3db7d9648/startexpt.sh)
that automates the process of starting the master, replicas, and clients.
In this script, we assume the following setting:
* `node-0`: the control node where we invoke `startexpt.sh`.
* `node-1`: the master node.
* `node-2,...,node-{n+1}`: `n` replica nodes
* `node-{2+n},...`: client nodes
* Copilot source code directory and binaries are placed in a shared location accessible via a Network File System (NFS)

```shell
Usage: ./startexpt.sh <protocol> <num-replicas> <num-client-instances> <num-client-nodes>
```
For example, the following command will set up a Copilot cluster of 5 replicas and 30 closed-loop client instances
sending their commands to these replicas. The client instances are evenly distributed across 10 nodes/machines.
```shell
# Run Copilot with 5 replicas and 30 clients (spread across 10 client nodes)
./startexpt.sh "copilot" 5 30 10
```
(Note: to run experiment with the Latent Copilot, specify `latentcopilot` instead of `copilot`. 
)
## Experiment Data
When `startexpt.sh` is executed, a random ID is generated for this experiment,
and a subdirectory that is named based on this ID is created under `experiments/`.
This subdirectory contains raw data&mdash;e.g., throughput and latency&mdash;from each client, and the overall data
that is output by the post-processing scripts.
* `percentilesnew.txt`: command latency (in microsecond) at every percentile
* `tput.txt`: system throughput (commands/second)
* `tputlat.txt`: system throughput and percentile latencies put together
* `sys_tput.txt`: instantaneous system throughput (interval is specified in `tput_interval_in_sec` in `startexpt.sh`)


# Other Notes
## Open-loop Clients
We also provide an implementation for open-loop clients,
which can be found at `src/clientol`.
An open-loop client sends commands in an open loop 
and the time between successive commands of a client follows an exponential distribution.
The rate parameter (commands/second) of the distribution can be specified using the option `-target_rps`.
```shell
# Run an open-loop client with the arrival rate of commands is 2000 commands/second
./bin/clientol -maddr=<master-ip> -mport=7087 -twoLeaders=true -id=0 -target_rps=2000
```
