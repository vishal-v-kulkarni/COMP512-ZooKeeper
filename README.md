# COMP512-ZooKeeper
Programming Assignment 3 of COMP-512 Fall 2022

**Disclaimer: I have created PA3 folder as parent of apache. So my commands include PA3 in them. Please remove according to your convenience.**

**Commands to run a Zookeeper Ensemble and connect Clients (in brief):
**

Go to particular lab folder in apache, then
To start: ~/PA3/apache-zookeeper-3.6.2-bin/bin/zkServer.sh start zoo-base.cfg

To stop: ~/PA3/apache-zookeeper-3.6.2-bin/bin/zkServer.sh stop zoo-base.cfg

**For clients to connect to ZK ensemble:**

Go to PA3 folder and run command:
~/PA3/apache-zookeeper-3.6.2-bin/bin/zkCli.sh -server lab2-13.cs.mcgill.ca:21810,lab2-14.cs.mcgill.ca:21810,lab2-15.cs.mcgill.ca:21810

**Compile Client, Server and Tasks files**

**Run this on all clients and zk ensemble servers:**
--> export ZOOBINDIR=~/PA3/apache-zookeeper-3.6.2-bin/bin
--> . $ZOOBINDIR/zkEnv.sh

**Client** - Go to zk/clnt and run: ./compileclnt.sh
**Server** - Go to zk/dist and run: ./compilesrvr.sh
**Task** - Go to zk/task and run: ./compiletask.sh

**To run server and client**
**Client** - Go to zk/clnt and run: ./runclnt.sh
**Server** - Go to zk/dist and run: ./runsrvr.sh
