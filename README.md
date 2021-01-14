<img src="./squirrel-small.png" align="right"
     alt="logo - image of squirrel with nut" width="200" height="161">

# WIP: Data Squirrel

WARNING: This project is a WORK IN PROGRESS file synchronization tool. Do not use it on any of your
data in its current state.

## Overview

Data Squirrel aims to be an offline first, peer to peer file synchronizer, heavily based on Tra
(https://pdos.csail.mit.edu/archive/6.824-2004/papers/tra.pdf).

Tra introduced a vector time algorithm for opportunistic file synchronization
(https://dspace.mit.edu/handle/1721.1/30527). This algorithm enables an architecture with
many sites holding a copy of a synchronized directory, where every site can synchronize its  local
changes with every other site in an arbitrary order. As long as no concurrent updates happen on
multiple stores at once, this sync between pairs of data stores will always succeed and eventually
all stores are up to date.

We find this idea interesting for an offline data synchronizer (i.e. synchronizing devices without
a central server) that keeps multiple sites with big amounts of data (e.g. Photo/Video vaults).
The idea is to have a media collection mirrored on multiple mass storage devices
(simple external hard drives) and to keep these collections in sync if files are edited or added. 
The medium to transfer data between these different data stores can be any other storage
device that synchronizes changed files to its local storage, is carried to all other stores and
finally updates the other stores with the changes. Concrete, the goal is to use an laptop that
is infrequently connected to the big data stores to keep them in sync by carrying the changes.

In contrast to existing data synchronizers, we believe that the vector time pair algorithm can be
easily modified in a way, that the laptop carrying the changes never stores the full data
collection. This can be helpful when for example synchronizing few terabytes of media with an
laptop that can only store about 200GB of data at once. The fact that this can be done without
a central server and with no active components on the storage endpoints (i.e. the hard drives
holding the big media collection), makes it interesting for medium sized home uses, that have
hard drives at a few physical locations and do not want to worry about keeping them in sync.
