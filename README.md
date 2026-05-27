# ergosphere

A reactive, event-driven push replication daemon for Pi-hole v6 replica setups.

> ⚠️ **Development Status**: This project is in early development. The core synchronization engine and async event loops
> are fully operational, but some quirks are still being ironed out. Expect significant changes and potential
> instability.
> Use with caution.

## Key Features

- **Reactive Architecture**: Built on an event-driven model, `ergosphere` reacts to changes in the Pi-hole database and
  filesystem in real-time, ensuring that replicas are always up-to-date with minimal latency.
- **Recursive Event Debouncing**: Features a self-resetting async sleep window to aggregate rapid cascading file writes
  into a single optimized sync sequence.
- **Reduced Network Overhead**: By batching updates and leveraging local filesystem checks rather than constant polling,
  `ergosphere` minimizes unnecessary network traffic and database queries.
- **Lightweight and Efficient**: Designed to run on low-resource environments, making it ideal for Raspberry Pi setups
  commonly used with Pi-hole.

## Architecture Overview

Unlike other sync scripts that rely on cron-job schedules or constant polling, `ergosphere` operates on a reactive event
loop. It listens for filesystem events on core Pi-hole files (like `gravity.db`), triggering
synchronization processes only when necessary. This approach allows for more efficient resource usage and faster
replication across multiple Pi-hole instances.

1. **Monitor:** A background worker thread tracks filesystem events inside the primary Pi-hole filesystem.
2. **Debounce:** When a change is detected, a self-resetting async sleep window is initiated. If additional changes
   occur during this window, the timer resets, allowing for multiple rapid changes to be consolidated into a single sync
   operation.
3. **Validate:** Once the directory goes quiet, the daemon checks the local modification attributes. If the aggregate
   checksum matches the memory registry, execution short-circuits.
4. **Replicate**: If a real state shift is verified, `ergosphere` triggers a single-flight network pipeline: pulling a
   Teleporter binary from the primary API, distributing it to replicas, and forcing individual thread-safe gravity table
   rebuilds over chunked HTTP streams.

## Configuration

Currently, during the pre-release phase, connection endpoints and authentication strings are managed via the development
test harness.

Planned configuration options for the upcoming `1.0.0` release include:

```toml
[daemon]
watch_directory = "/etc/pihole"
debounce_seconds = 3

[primary]
url = "http://192.168.1.10:8080"
password = "password"
label = "primary-node"

[[replicas]]
url = "http://192.168.1.11:8081"
password = "password"
label = "replica-node"

[sync]
full_sync = false
config = false
dhcp_leases = false

[sync.gravity]
group = true
adlist = true
adlist_by_group = true
domain_list = true
domain_list_by_group = true
client = true
client_by_group = true
```