# Handout: a read-only pipeline monitor

## Goal

Provide a human with a live view of pipeline health without managing queues or
changing how topic messages are delivered. The monitor should not copy,
serialize, intercept, or resend topic payloads.

The design is observational: sample the operating system's view of the pipe
buffers already used for topic subscriptions, and sample resource usage for
each plugin worker process. Display approximate backlog, trends, and resource
usage, along with shared-environment activity.

## How topic delivery works

Each topic subscription uses a pipe-backed `Queue`. A publisher writes to that
subscriber's pipe, and the subscriber's worker reads messages and invokes its
callback. The manager tracks topic/subscriber information and holds the
subscription's queue descriptor. See [plugin_data.py](../parallelazy/plugin_data.py),
[plugin_worker.py](../parallelazy/plugin_worker.py), and
[plugin_manager.py](../parallelazy/plugin_manager.py).

This is not a `multiprocessing.Queue` with a public `qsize()` method. The
underlying transport is a `multiprocessing.Pipe`, so the operating system can
report how many bytes are waiting to be read from its read end.

## What the monitor can estimate

On Linux, the manager can sample unread bytes for each subscriber pipe with a
non-consuming query such as `FIONREAD`. Sampling does not read or alter the
messages. Over time, the monitor can show:

- **Pending bytes:** bytes currently waiting in the pipe.
- **Backlog trend:** whether pending bytes are growing, shrinking, or staying
  roughly level.
- **Approximate queued messages:** pending bytes divided by a known or
  representative encoded message size. This is most useful when messages on a
  topic have similar sizes.
- **Approximate net flow imbalance:** the change in estimated queued messages
  over a time interval. A growing backlog means the producer is outpacing that
  consumer; a shrinking backlog means the consumer is catching up.

Sample each subscriber separately. One slow subscriber can fall behind even
when the other subscribers to the same topic are keeping up.

## What it cannot tell you

Pipe occupancy is **not an exact queue length**:

- The operating system reports bytes, not message boundaries. Message-count
  estimates depend on encoded message sizes and can be poor for variable-size
  payloads.
- A message already read from the pipe but still being processed by its callback
  is no longer counted in the pipe backlog.
- A snapshot shows only the current buffer state. Short bursts between samples
  may be missed.
- The change in backlog is the **net difference** between producer and
  consumer flow. It cannot reveal their individual message rates. An empty
  pipe does not mean there is no traffic; the producer and consumer may simply
  be keeping pace.
- Pipe bytes describe IPC data, not necessarily the logical size of a payload.
  For example, tensor transport may use shared memory or CUDA IPC.

If accurate per-message counts, individual producer/consumer rates, or callback
processing time become requirements, those would need separate instrumentation.
They are deliberately outside this monitor's initial design.

## Performance and manager integration

The monitor should query pipe state periodically, rather than do work for every
message. It should never drain a pipe, inspect or serialize a payload, or send
telemetry through a topic. Its work is limited to a small operating-system
query per subscription per refresh.

The monitor is a separate, optional Python module loaded into the manager
process. It is enabled per manager instance; it is not a subprocess in the
initial design. The module can sample manager-held pipe references and read
process and environment metadata through a manager adapter.

Keep the integration narrow and the module independently usable:

- The monitor owns metric models, sampling policy, and the human-facing view.
- A manager adapter provides read-only access to topic subscriptions, worker
  PIDs, and environment state. Keep manager-specific access inside this
  adapter rather than coupling the view to manager internals.
- The monitor is ordinary package code imported normally; no runtime
  `importlib` loading or unloading is needed. Importing the module must not
  start sampling or create background threads.
- Create the monitor instance on demand when monitoring is enabled at runtime,
  rather than requiring it in `PluginManager.__init__`. A minimal API could be
  `enable_monitoring()` and `disable_monitoring()`: enabling creates and starts
  the sampler, while disabling stops sampling and releases the instance.
  Monitoring remains active independently of whether a report is currently
  being displayed.
- Keep sampled data in simple snapshot records so it can later cross a process
  boundary. If sampling moves to a subprocess, either pass duplicated pipe
  handles once or keep pipe probing in the manager and send only small metric
  snapshots to the monitor.

When monitoring is disabled, there should be no sampler or per-message topic
instrumentation. When enabled, sampling continues at its configured interval
whether or not the user has requested output. An optional key listener can
trigger reporting, but it does not start or stop sampling. The manager's
publish path, worker protocol, and queue behavior do not need to change.

Lifecycle operations should be safe to repeat and synchronized with manager
shutdown and plugin unload. Stop the sampler before manager pipe references
are closed. Obtain a consistent read-only snapshot of changing manager state,
then release manager locks before OS sampling and rendering. Keep sampling
intervals modest and measure overhead with monitoring enabled and disabled;
in-process sampling still consumes some manager CPU time.

The initial pipe-inspection approach is platform-specific. Linux supports
`FIONREAD` for pipe byte counts; other platforms, particularly Windows, need a
separate implementation or should report that this metric is unavailable.

## Passive process resource monitoring

The manager already knows each worker process ID through its plugin status
data. A monitor can use that ID to query operating-system process statistics
without asking the plugin to collect or send telemetry. The manager exposes
the process ID in `ProcessStatus`; see [plugin_data.py](../parallelazy/plugin_data.py)
and [plugin_manager.py](../parallelazy/plugin_manager.py).

| Resource | Possible observation | Important limitation |
| --- | --- | --- |
| CPU | Sum CPU-time deltas for the plugin worker and its live descendants over a wall-clock interval. | This is process-tree usage, not per-thread or per-plugin-function usage. A process tree using multiple cores can exceed 100%, depending on the displayed convention. |
| RAM | Sum resident set size (RSS) for the plugin worker and its live descendants; optionally report proportional set size (PSS) where available. | Summed RSS can count shared pages more than once. PSS apportions shared pages but can cost more to collect and is not equally available on all platforms. |
| GPU memory | Sum per-process memory for the worker and its live descendants using a vendor's GPU management API, such as NVIDIA's NVML, when supported. | Availability depends on GPU, driver, permissions, and configuration; a child PID may not be exposed by the vendor API. |
| GPU utilization | Report per-process utilization for the worker and its live descendants only when the GPU and driver expose it. | Many systems expose device-wide utilization but not a reliable per-process value. Do not label device-wide utilization as a plugin's utilization. |

This approach does not require plugin callbacks, per-message counters, or
payload inspection. CPU and RAM sampling can use operating-system process
statistics. GPU monitoring should use a management API rather than invoking
CUDA work in the manager; the monitor should not create a CUDA context or
synchronize a plugin's GPU.

All plugins are started with the `spawn` start method, and a plugin may itself
spawn subprocesses. Process resource metrics must therefore cover the worker
PID registered by the manager and its recursive live descendants, not just the
worker. Use process-tree discovery (for example,
`psutil.Process(pid).children(recursive=True)`) at each sample, deduplicate
PIDs, and label aggregate values as covering the worker and descendants.
Calculate CPU by retaining per-PID cumulative samples and summing their
interval deltas so a child appearing or disappearing does not make the
aggregate counter go backwards. Sum current RSS across the discovered tree and
label it as aggregate RSS; shared memory may be counted multiple times. Query
GPU metrics for every discovered PID and aggregate only metrics that the vendor
API actually reports.

Process-tree sampling is necessarily approximate: a short-lived child that
starts and exits between samples can be missed, and a child that outlives its
worker may be reparented and no longer discoverable from that worker PID.
Document this sampling limitation rather than presenting the totals as exact.
GPU metrics may be unavailable, in which case the view should say so instead
of displaying a misleading zero.

Optional monitoring backends must fail independently: if a dependency is
missing, mark only the metrics that depend on it as unavailable and continue
sampling metrics provided by other backends. Do not silently report zero or
prevent the monitor from starting. Use `psutil` for recursive process-tree
discovery and cross-platform process statistics (`pip install psutil`), and
`nvidia-ml-py` for NVIDIA NVML metrics (`pip install nvidia-ml-py`). If `psutil`
is missing, mark worker-plus-descendant process metrics unavailable with its
install command; do not silently report worker-only values as group totals.
Whenever any optional package is missing, include that specific package and
its `pip install <package>` command in the unavailable status. The report
should explain which capability is missing and how to enable it. Do not install
dependencies automatically. Import optional packages lazily when their backend
is initialized so importing the monitor remains safe without those packages.

## Shared environment monitoring

The shared environment is an in-memory mapping owned by the manager. Normal
plugin `env_set` calls already pass through the manager, which stores the
latest value and notifies subscribers. This gives a monitor a central place to
observe environment state without adding messages between plugins or changing
topic delivery. See [environment_manager.py](../parallelazy/environment_manager.py)
and [plugin_manager.py](../parallelazy/plugin_manager.py).

The current environment keeps only the latest value for each key; it does not
retain update history or per-key update counts. A monitor can read a snapshot
to display the number of keys, names, and value types. To show update rates, add
a small counter and last-write timestamp at the manager's existing `env_set`
path. Count writes rather than comparing arbitrary values: equality checks can
be expensive or undefined for values such as tensors. This adds a tiny amount
of work to an operation already handled by the manager, but does not require
plugins to send extra telemetry.

There are two different size metrics:

- **IPC update size:** the encoded byte length of a plugin-originated
  environment update as it crosses the manager's RPC pipe. This can be a
  useful measure of transport volume, but it is not the in-memory size of the
  value; manager-local `env_set` calls have no IPC size. The current transport
  does not expose the encoded length to the environment handler. To measure it
  without serializing the value a second time, capture the encoded length at
  the existing serialization/send boundary. Calling `pickle.dumps` solely to
  measure each update would add serialization work and should be avoided.
- **Stored-value size:** an estimate of memory occupied by the value in the
  manager. A shallow size can be reported without serializing values or
  recursively walking large object graphs, but it excludes nested contents.
  Recursive sizing may be expensive, double-count shared objects, or be
  unreliable for custom objects. Tensor logical size is not necessarily the
  physical memory used by the process or GPU.

Keep values themselves hidden by default; show names, types, and size estimates
so the monitor does not expose application data.

## Sampling and reporting

Sampling and reporting are separate:

- **Sampling:** when monitoring is enabled, a background sampler periodically
  collects metrics and retains the latest snapshot and the previous sample
  needed to calculate rates and trends. Sampling continues even when nothing
  is being printed.
- **On-demand snapshot:** the application can request a point-in-time
  `MonitoringSnapshot` for its own use. It should not print into the manager's
  terminal by default. A one-off request while monitoring is disabled can
  return current values, but cannot provide historical rates or trends.
- **Key-triggered report:** an optional key listener prints the latest sampled
  snapshot to the manager's terminal when requested. It does not change the
  sampler's state or print periodic updates, so normal logs remain undisturbed
  between keypresses.
- **Other renderers:** the same snapshot model can later feed a browser view
  or another UI without changing the sampler.

Represent backend availability in the snapshot separately from metric values.
When an optional dependency or platform capability is unavailable, the
snapshot and report should include the affected capability, an unavailable
reason, and an installation command for the missing optional package. For
example: `GPU metrics unavailable (NVML support is not installed). Enable with:
pip install nvidia-ml-py`. Apply this consistently to every optional package,
including `psutil` (`pip install psutil`) when process metrics use it. Do not
show unavailable metrics as zero, and do not hide the notice merely because
other metrics were collected successfully. An installation hint is
informational only; the application or user remains responsible for
installing the package. For platform capabilities with no installable package,
state that the metric is unsupported on the current platform instead of
showing an irrelevant install command.

A first sample should mark rate fields as unavailable. Subsequent samples can
compare monotonic timestamps, cumulative CPU times, and pipe-byte readings.
Environment write rates additionally require a small counter at the manager's
existing `env_set` path while monitoring is enabled; earlier writes cannot be
reconstructed. Keep only the previous sample unless the user explicitly
requests history.

The snapshot data model should be independent of its presentation. A keypress
prints a compact report from the latest sample; it should not trigger fresh
sampling or turn the terminal into a continuously redrawn dashboard.

There is no universally unused shortcut. In particular, Ctrl+M is normally
the same carriage-return key as Enter and should not be the default. Keyboard
capture is only possible in the terminal running the manager, when it has an
interactive TTY. Reading a key immediately requires terminal input handling
that can conflict with `input()`, readline, or another UI. Therefore:

- Keep the hotkey listener opt-in and allow the key to be configured.
- If the application already handles keyboard input, let it call the
  monitor's report operation instead of having the monitor read stdin.
- Otherwise, use a terminal-specific listener only when stdin is a TTY, and
  restore terminal settings when the listener is unregistered, on shutdown,
  and after errors. While the listener is active, terminal key-reading mode
  may affect other stdin use.
- Keep the report compact so it remains useful among existing logs. Do not
  clear or take over the terminal screen.

## Suggested first view

A key-triggered report can show compact topic, process, and environment
summaries:

The values below are illustrative, not measurements from a running pipeline.

| Topic | Publisher | Subscriber | Pending bytes | Estimated messages | Trend |
| --- | --- | --- | ---: | ---: | --- |
| `events` | `source` | `worker-a` | 0 | 0 | steady |
| `events` | `source` | `worker-b` | 8192 | ~8 | growing |

| Plugin | Root PID | Scope | CPU | RAM (RSS) | GPU memory | GPU utilization |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| `worker-a` | 12345 | worker + descendants | 45% | 1.2 GiB | 2.0 GiB | unavailable |

| Environment key | Type | Writes/s | IPC update size | Stored size | Subscribers |
| --- | --- | ---: | ---: | ---: | ---: |
| `pipeline.stage` | `str` | 0.2 | ~92 B (encoded) | 56 B (shallow) | 2 |

If GPU support is unavailable because its optional dependency is missing, the
GPU fields should instead show an actionable notice, for example:
`unavailable — install with pip install nvidia-ml-py`. Display the actual
missing package; never show a guessed or stale installation command. Use the
same format for every other missing optional package, such as
`unavailable — install with pip install psutil`.

Label message counts as estimates, and show pending bytes as the primary
measurement. Do not present backlog trend as an exact producer or consumer
rate. Show unavailable process or GPU metrics as unavailable, not as zero.

Before relying on the monitor in production, compare pipeline throughput and
publish latency with monitoring disabled and enabled, using representative
message types and rates. Also compare CPU and GPU workloads with resource
sampling disabled and enabled. The intended overhead is small and off the
topic data path, but it should be measured on supported platforms.

## Scope boundary

This monitor does not change queue capacity, block or throttle producers,
discard messages, rebalance consumers, or guarantee delivery. Queue management
and flow control are separate future concerns.
