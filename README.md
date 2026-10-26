# Circuit breaker restarts leak RabbitMQ consumers; their messages are acked but never handled

When a circuit breaker pauses a RabbitMQ listener more than once per trip, the restart leaves extra consumers on the
queue. Some of them deliver into a drained receiver, which acks the message and drops it: no handler, no error, no
dead letter, nothing unacked.

Reproduced on **WolverineFx 6.54.0**, on every run.

## Run it

Requires Docker and the .NET 10 SDK.

```sh
docker compose up -d
dotnet run --project src/Repro
```

One RabbitMQ listener with `ListenerCount(2)`, `BufferedInMemory()`, a `CircuitBreaker` (20 s pause) and
`RetryWithCooldown(2s, 5s)`. The app publishes 30 failing messages, waits for two pause/restart cycles, then publishes
100 good messages and compares the broker's per-consumer statistics with what the handler received. Takes about two
minutes; Wolverine's debug log is written to `repro-debug.log`.

## Result

Expected: 2 consumers after every restart, 100 of 100 handled.

```
11:14:23 consumers on 'ghost-consumer-repro': 2 (expected 2)
11:14:23 publishing 30 failing messages to trip the circuit breaker
11:14:32 info: ListeningAgent: Pausing message listening at rabbitmq://queue/ghost-consumer-repro
11:14:32 info: ListeningAgent: Pausing message listening at rabbitmq://queue/ghost-consumer-repro
11:14:32 info: ListeningAgent: Pausing message listening at rabbitmq://queue/ghost-consumer-repro
11:14:52 info: ListeningAgent: Started message listening at rabbitmq://queue/ghost-consumer-repro
11:14:52 info: ListeningAgent: Started message listening at rabbitmq://queue/ghost-consumer-repro
11:14:53 consumers on 'ghost-consumer-repro': 2
11:14:59 info: ListeningAgent: Pausing message listening at rabbitmq://queue/ghost-consumer-repro
11:14:59 info: ListeningAgent: Pausing message listening at rabbitmq://queue/ghost-consumer-repro
11:14:59 info: ListeningAgent: Pausing message listening at rabbitmq://queue/ghost-consumer-repro
11:15:19 info: ListeningAgent: Started message listening at rabbitmq://queue/ghost-consumer-repro
11:15:19 info: ListeningAgent: Started message listening at rabbitmq://queue/ghost-consumer-repro
11:15:24 consumers on 'ghost-consumer-repro': 6
11:15:44 publishing 100 good messages

Per consumer, for the good messages (from the broker's channel statistics):
  172.20.0.1:43268 -> 172.20.0.2:5672 (2)       delivered  17, acked  17
  172.20.0.1:43268 -> 172.20.0.2:5672 (3)       delivered  17, acked  17
  172.20.0.1:43268 -> 172.20.0.2:5672 (4)       delivered  17, acked  17
  172.20.0.1:43268 -> 172.20.0.2:5672 (5)       delivered  17, acked  17
  172.20.0.1:43268 -> 172.20.0.2:5672 (6)       delivered  16, acked  16
  172.20.0.1:43268 -> 172.20.0.2:5672 (7)       delivered  16, acked  16

Consumers on the queue:          6 (expected 2)
Good messages handled:           66 of 100
Receipts logged by Wolverine:    100 ("Received message" debug lines)
Left on the queue:               0
Unacked on the queue:            0
New dead letters:                0
BUG REPRODUCED: 4 leaked consumer(s), 34 message(s) acked but never handled
```

The numbers vary between runs because it's a race (2 to 4 leaked consumers, 25 to 34 lost).

## When it happens

Settings can be changed with `REPRO_LISTENERS`, `REPRO_MODE` (`buffered` | `inline`) and `REPRO_RETRY` (`on` | `off`).

| Listeners | Mode | Retries | "Pausing" per trip | Leaked consumers | Lost (of 100) |
|---|---|---|---|---|---|
| 2 | buffered | on | 3 | 4 | 34 |
| 1 | buffered | on | 3 | 2 | 34 |
| 2 | buffered | off | 1 | 0 | 0 |
| 1 | inline | off | 1 | 0 | 0 |

`ListenerCount` doesn't matter. It needs more than one pause per trip, which here came from a buffered listener whose
failing messages were still retrying when it paused.

## Cause

Links are to [V6.54.0](https://github.com/JasperFx/wolverine/tree/V6.54.0/src/Wolverine).

1. Failures still in flight when the listener stops are reported to the breaker, and each report calls
   `PauseWithDrainAsync` again (three "Pausing" lines per trip).
2. Every pause installs a new restart timer without disposing the previous one
   ([`ListeningAgent.cs#L820`](https://github.com/JasperFx/wolverine/blob/V6.54.0/src/Wolverine/Transports/ListeningAgent.cs#L820)).
   The inbox recovery path does dispose it
   ([#L843](https://github.com/JasperFx/wolverine/blob/V6.54.0/src/Wolverine/Transports/ListeningAgent.cs#L843)).
3. When the pause ends, each timer calls `StartAsync`, which checks `Status` without a lock
   ([#L471](https://github.com/JasperFx/wolverine/blob/V6.54.0/src/Wolverine/Transports/ListeningAgent.cs#L471)).
   Each call builds a full set of listeners; `Listener` keeps the last, the other set is never stopped (two "Started
   message listening" lines per restart).
4. The next pause stops only the current `Listener` and drains the receiver
   ([#L404](https://github.com/JasperFx/wolverine/blob/V6.54.0/src/Wolverine/Transports/ListeningAgent.cs#L404)).
   Consumers leaked in the earlier cycle keep delivering into that drained receiver.
5. `BufferedReceiver.ReceivedAsync` posts to the completed `_receivingBlock`, which drops it silently, then posts to
   `_completeBlock`, which acks it
   ([`BufferedReceiver.cs#L412`](https://github.com/JasperFx/wolverine/blob/V6.54.0/src/Wolverine/Runtime/WorkerQueues/BufferedReceiver.cs#L412)).
   In the run above all 100 messages reached `BufferedReceiver`, only 66 reached the handler pipeline, and the broker
   shows every delivery acked. Consumers leaked in the latest restart still feed a live receiver, which is why only
   part of the leaked consumers lose messages.

## Possible fix

Untested.

- Dispose the previous `Restarter` before installing a new one, or ignore a pause while already `Paused`.
- Serialize `StartAsync` and re-check `Status` inside the lock.
- A drained `BufferedReceiver` could reject a post instead of acking a message it has dropped.
