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

Configuration options can be specified in the following ways (in ascending order of precedence):

* In `~/.config/ergosphere/config.toml`
* In a file named `config.toml` in the current working directory (useful for testing).
* Environment variables.

> [!NOTE]
> All the sources mentioned above are layered together, with each successive source overriding options from the previous
> one.
> This also means that if a source does *not* override a specific option from the previous source, then that option
> retains its value from the previous source without being overridden. Think of it as the sources being merged together
> as an intersection.

The following are the configuration options that can be specified (options marked with * are required):

### `[daemon]`

| Name                           | Environment Variable                              | Default       | Example        | Description                                                                   |
|--------------------------------|---------------------------------------------------|---------------|----------------|-------------------------------------------------------------------------------|
| `client_timeout_seconds`       | `ERGOSPHERE_DAEMON__CLIENT_TIMEOUT_SECONDS`       | 20            | 5              | The timeout (in seconds) for the HTTP client.                                 |
| `client_skip_tls_verification` | `ERGOSPHERE_DAEMON__CLIENT_SKIP_TLS_VERIFICATION` | false         | true           | Whether the HTTP client should skip TLS verification.                         |
| `debounce_seconds`             | `ERGOSPHERE_DAEMON__DEBOUNCE_SECONDS`             | 3             | 1              | Safety sleep duration window to absorb rapid filesystem<br/>cascading writes. |
| `watch_directory`              | `ERGOSPHERE_DAEMON__WATCH_DIRECTORY`              | "/etc/pihole" | "./pihole"     | Pi-hole config directory                                                      |
| `timezone`                     | `ERGOSPHERE_DAEMON__TIMEZONE`                     | "UTC"         | "Asia/Kolkata" | Timezone for logging timestamps (IANA format)                                 |

### `[primary]`

| Name        | Environment Variable           | Default                        | Example              | Description                                              |
|-------------|--------------------------------|--------------------------------|----------------------|----------------------------------------------------------|
| `label`     | `ERGOSPHERE_PRIMARY__LABEL`    | The node's IP address/hostname | "pihole-primary"     | A friendly identifier for the primary node.              |
| `url`*      | `ERGOSPHERE_PRIMARY__URL`      | null                           | "http://192.168.0.2" | The endpoint URL for the primary node.                   |
| `password`* | `ERGOSPHERE_PRIMARY__PASSWORD` | null                           | "password"           | The web UI or application password for the primary node. |

### `[[replicas]]`

| Name        | Environment Variable                                                                                                     | Default                        | Example              | Description                                                    |
|-------------|--------------------------------------------------------------------------------------------------------------------------|--------------------------------|----------------------|----------------------------------------------------------------|
| `label`     | `ERGOSPHERE_REPLICAS__0__LABEL`<br/>`ERGOSPHERE_REPLICAS__1__LABEL`<br/>...<br/>`ERGOSPHERE_REPLICAS__N__LABEL`          | The nodes' IP address/hostname | "pihole-replica"     | A friendly identifer for the replica node.                     |
| `url`*      | `ERGOSPHERE_REPLICAS__0__URL`<br/>`ERGOSPHERE_REPLICAS__1__URL`<br/>...<br/>`ERGOSPHERE_REPLICAS__N__URL`                | null                           | "http://192.168.0.3" | The endpoint URL(s) for the replica node(s).                   |
| `password`* | `ERGOSPHERE_REPLICAS__0__PASSWORD`<br/>`ERGOSPHERE_REPLICAS__1__PASSWORD`<br/>...<br/>`ERGOSPHERE_REPLICAS__N__PASSWORD` | null                           | "password"           | The web UI or application password(s) for the replica node(s). |

### `[sync]`

> [!NOTE]
> If `full_sync` is `true`, then all further sync options (except `run_gravity`) are forced to `true`.

| Name          | Environment Variable           | Default | Example | Description                                                                             |
|---------------|--------------------------------|---------|---------|-----------------------------------------------------------------------------------------|
| `run_gravity` | `ERGOSPHERE_SYNC__RUN_GRAVITY` | false   | true    | Whether to run the gravity update action on<br/>the replica node after synchronization. |
| `full_sync`   | `ERGOSPHERE_SYNC__FULL_SYNC`   | true    | false   | Synchronize everything (i.e. enable all Teleporter<br/>import options).                 |
| `config`      | `ERGOSPHERE_SYNC__CONFIG`      | true    | false   | Synchronize Pi-hole configuration.                                                      |
| `dhcp_leases` | `ERGOSPHERE_SYNC__DHCP_LEASES` | true    | false   | Synchronize DHCP leases.                                                                |

### `[sync.gravity]`

| Name                  | Environment Variable                            | Default | Example | Description                        |
|-----------------------|-------------------------------------------------|---------|---------|------------------------------------|
| `group`               | `ERGOSPHERE_SYNC__GRAVITY__GROUP`               | true    | false   | Synchronize groups.                |
| `adlist`              | `ERGOSPHERE_SYNC__GRAVITY__ADLIST`              | true    | false   | Synchronize ad lists.              |
| `adlist_by_group`     | `ERGOSPHERE_SYNC__GRAVITY__ADLIST_BY_GROUP`     | true    | false   | Synchronize ad lists by group.     |
| `domainlist`          | `ERGOSPHERE_SYNC__GRAVITY__DOMAINLIST`          | true    | false   | Synchronize domain lists.          |
| `domainlist_by_group` | `ERGOSPHERE_SYNC__GRAVITY__DOMAINLIST_BY_GROUP` | true    | false   | Synchronize domain lists by group. |
| `client`              | `ERGOSPHERE_SYNC__GRAVITY__CLIENT`              | true    | false   | Synchronize clients.               |
| `client_by_group`     | `ERGOSPHERE_SYNC__GRAVITY__CLIENT_BY_GROUP`     | true    | false   | Synchronize clients by group.      |

### Example Configuration

```toml
[daemon]
client_timeout_seconds = 20
client_skip_tls_verification = false
debounce_seconds = 3
watch_directory = "/etc/pihole"
timezone = "Asia/Kolkata"

[primary]
label = "pihole-primary"
url = "http://192.168.0.2"
password = "password"

[[replicas]]
label = "pihole-replica1"
url = "http://192.168.0.3"
password = "password"

[[replicas]]
label = "pihole-replica2"
url = "http://192.168.0.4"
password = "password"

[sync]
run_gravity = true
full_sync = false
config = false
dhcp_leases = false

[sync.gravity]
group = true
adlist = true
adlist_by_group = true
domainlist = true
client = true
client_by_group = true
```