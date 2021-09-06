Copilot
======


### What is Copilot?
Copilot replication is the first 1-slowdown-tolerant consensus protocol: it delivers normal latency despite the slowdown of any 1 replica.

### What makes Copilot novel? 
No existing consensus protocol is slowdown-tolerant: a single slow replica can sharply increase their latency. 
Copilot is the first 1-slowdown-tolerant consensus protocol. 
It avoids slowdowns using two distinguished replicas, the pilot and copilot.
Its pilot and copilot both receive, order, execute, and reply to all client commands. 
It uses this proactive redundancy and a fast takeover mechanism that allows a fast pilot to safely complete 
the work of a slow pilot to provide slowdown tolerance. 

It has two optimizations&mdash;ping-pong batching and null dependency elimination&mdash;that improve its performance 
when there are 0 and 1 slow pilots respectively. 
Despite its redundancy, Copilot replication's performance is competitive 
with existing consensus protocols when no replicas are slow. 
When a replica is slow, Copilot is the only consensus protocol that avoids high latencies for client commands.


### How does Copilot work?

Our [OSDI 2020 paper](https://www.usenix.org/conference/osdi20/presentation/ngo) describes the motivation, design, implementation, and evaluation of Copilot.

### What is Latent Copilot?
Latent Copilot, a variant of Copilot, is another design and implementation of a 1-slowdown-tolerant consensus protocol.
Latent Copilot operates with one active pilot, which actively proposes commands, and one latent pilot,
which proposes commands only when they have not been committed by the active pilot in a timely manner. 
In this way, Latent Copilot achieves an intermediate tradeoff 
between MultiPaxos and Copilot in terms of throughput and slowdown tolerance.

Latent Copilot has different mechanisms to determine when a pilot should switch
its mode if it suspects the other pilot is continually slow or fast.
To learn about the progress of the other pilot, a pilot uses additional metadata embedded in the ordering messages to
learn about the status of the commands from the replicas.

For more details about Latent Copilot, please refer to Ngo's Ph.D. dissertation.

### What is in this repository?

This repository contains the Go implementations of:

* Copilot

* Latent Copilot

* EPaxos

* (classic) Paxos

* Mencius

* Generalized Paxos

The implementations of EPaxos, MultiPaxos, Mencius, and Generalized Paxos were created by Iulian Moraru, David G. Andersen, and Michael Kaminsky as part of the [EPaxos project](https://github.com/efficient/epaxos).

The struct marshaling and unmarshaling code was generated automatically using
the tool available at: https://code.google.com/p/gobin-codegen/


AUTHORS:

Khiem Ngo -- Princeton University

Siddhartha Sen -- Microsoft Research

Wyatt Lloyd -- Princeton University


