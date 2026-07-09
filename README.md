<<<<<<< HEAD
# High-Throughput-Log-Aggregator


### Wha is meant by High Throughput
<p>High throughput means the system can handle a large number of log events per second continuously, not just in a short burst.

### What is the that my laptop can handle


### What are the possible choking points
## 1. Log producer
The app itself may generate logs too slowly or too fast.
Bad logging code can block the request path. That is fatal in high-throughput systems.

## 2. Collector / agent
This is where logs are picked up, parsed, batched, compressed, or forwarded.
If the collector is synchronous or too CPU-heavy, it becomes the bottleneck.

## 3. Network
Even locally, container-to-container traffic can choke if you send too many tiny messages instead of batching.

## 4. Queue / stream
If the queue cannot absorb bursts, logs pile up at the producer.
If the queue is too small, you lose the whole point of buffering.

## 5. Processor
Parsing JSON, adding metadata, filtering noise, and enriching records all cost CPU.
Bad regexes and huge payloads will hurt badly.

## 6. Storage
Writing every log one by one to disk or a database will collapse quickly.
You need batching, indexing discipline, and retention rules.

## 7. Query layer / dashboard
Even if ingestion is fine, Grafana or your query API can become slow when the stored dataset grows.


### End to End measuring
### Ingest metrics
<li>events/sec accepted

<li>bytes/sec accepted

<li>dropped events

<li>retry count

<li>Latency metrics

<li>producer timestamp → collector receive time

<li>collector receive time → queue publish time

<li>queue publish time → processor start time

<li>processor start time → storage write time

<li>storage write time → query availability time

#### Quality metrics

<li>loss rate

<li>duplicate rate

<li>ordering errors

<li>malformed log rate

#### System metrics

<li>CPU usage

<li>RAM usage

<li>disk I/O

<li>network I/O

<li>queue depth / lag

<li>container restarts


## What is this project for
<p> collect logs from one or more apps, move them through a scalable pipeline,buffer bursts,process them asynchronously,store them reliably,and visualize/search them later.

## USER STORY


## TRADITIONAL SOLUTION


## PROBLEMS WITH TRADITIONAL SOLUTION


## PROPOSED SOLUTION


## HIGH LEVEL ARCHITECTURE

=======
# High Throughput Log Aggregator
>>>>>>> origin/main
