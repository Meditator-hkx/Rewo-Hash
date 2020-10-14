# Rewo-Hash
A Read-Efficient and Write-Optimized Hash Table for Commercial Non-Volatile Memory


## Overview

We develop a novel hash table, namely Rewo-Hash, for the commercial NVM product, Intel Opaten DC Persistent Memory (AEP). The main features of Rewo-Hash are five-fold:

- Cached Table-Inclined Read (assisted with DRAM space)
- Log-Free Atomic Write
- Efficient Shadowing Synchronization (between Persistent Table and Cached Table)
- Lightweight Non-Blocking Resizing
- High-Performance Concurrent Access


## How to Use

The "rewo-single" project only supports single-thread read/write access. It disables the locking mechanism to exploit high performance.

The "rewo-concurrent" project supports correct and low-overhead concurrent read/write access.

To build the project, just try "make" in the corresponding project.

Notice that the parameters in config.h file can be modified according to your own requirements.
