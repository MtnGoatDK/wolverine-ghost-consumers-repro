// Repro: a RabbitMQ listener with a circuit breaker leaks consumers when the breaker trips more than once
// per pause, and messages delivered to the leaked consumers are never handled.
//
// 1. Start with ListenerCount consumers.
// 2. Publish failing messages. Retries keep failing while the listener is paused, so the breaker trips
//    again during the pause, and every trip installs its own Restarter.
// 3. When the pause ends, the restarters all call ListeningAgent.StartAsync and build listeners in
//    parallel. Only the last set is kept; the others are never stopped, so their consumers stay on the queue.
// 4. Publish good messages, count how many are handled, and show per consumer what the broker delivered
//    and what got acked.
//
// Settings (environment variables): REPRO_LISTENERS (default 2), REPRO_MODE = buffered | inline
// (default buffered), REPRO_RETRY = on | off (default on).

using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using JasperFx.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Console;
using Wolverine;
using Wolverine.ErrorHandling;
using Wolverine.RabbitMQ;

const string QueueName = "ghost-consumer-repro";
const int FailingMessages = 30;
const int GoodMessages = 100;

var listenerCount = int.Parse(Environment.GetEnvironmentVariable("REPRO_LISTENERS") ?? "2");
var mode = Environment.GetEnvironmentVariable("REPRO_MODE") ?? "buffered";
var retry = (Environment.GetEnvironmentVariable("REPRO_RETRY") ?? "on") == "on";
var amqp = Environment.GetEnvironmentVariable("RABBITMQ_URI") ?? "amqp://repro:repro@localhost:5680";
var management = Environment.GetEnvironmentVariable("RABBITMQ_MANAGEMENT") ?? "http://localhost:15680";
var pauseTime = TimeSpan.FromSeconds(20);

using var http = new HttpClient { BaseAddress = new Uri(management) };
http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic",
    Convert.ToBase64String(Encoding.ASCII.GetBytes($"{new Uri(amqp).UserInfo}")));

// Right after `docker compose up -d` the broker needs a few seconds before it accepts connections.
Console.WriteLine($"Waiting for RabbitMQ at {management} ...");
for (var attempt = 0; ; attempt++)
{
    try
    {
        (await http.GetAsync("/api/overview")).EnsureSuccessStatusCode();
        break;
    }
    catch when (attempt < 60)
    {
        await Task.Delay(1000);
    }
}

var wolverineLog = new CapturingLoggerProvider();

using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        // Console: only "Pausing message listening" and "Started message listening". The failing handlers' own
        // error logs are expected and hidden. Everything Wolverine logs at Debug goes to repro-debug.log.
        logging.ClearProviders();
        logging.AddSimpleConsole(o => { o.SingleLine = true; o.TimestampFormat = "HH:mm:ss "; });
        logging.AddProvider(wolverineLog);
        logging.SetMinimumLevel(LogLevel.Debug);
        logging.AddFilter<ConsoleLoggerProvider>((category, level) =>
            category == "Wolverine.Transports.ListeningAgent" && level >= LogLevel.Information);
        logging.AddFilter<CapturingLoggerProvider>((category, _) => category?.StartsWith("Wolverine") == true || category == "Work");
    })
    .UseWolverine(opts =>
    {
        opts.UseRabbitMq(new Uri(amqp)).AutoProvision().AutoPurgeOnStartup();

        opts.PublishMessage<Work>().ToRabbitQueue(QueueName);

        var listener = opts.ListenToRabbitQueue(QueueName).ListenerCount(listenerCount);
        if (mode == "inline")
        {
            listener.ProcessInline();
        }
        else
        {
            listener.BufferedInMemory();
        }

        listener.CircuitBreaker(cb =>
        {
            cb.PauseTime = pauseTime;
            cb.MinimumThreshold = 10;
            cb.FailurePercentageThreshold = 20;
        });

        // Retries that keep failing while the listener is paused are what trip the breaker a second time.
        if (retry)
        {
            opts.Policies.OnException<InvalidOperationException>()
                .RetryWithCooldown(2.Seconds(), 5.Seconds())
                .Then.MoveToErrorQueue();
        }
    })
    .Build();

async Task<JsonElement[]> QueueConsumersAsync()
{
    // The consumer list is read live from the broker, unlike the queue's "consumers" statistic.
    var consumers = await http.GetFromJsonAsync<JsonElement>("/api/consumers/%2F");
    return consumers.EnumerateArray().Where(c => c.GetProperty("queue").GetProperty("name").GetString() == QueueName).ToArray();
}

async Task<int> ConsumersAsync() => (await QueueConsumersAsync()).Length;

// Per consumer channel: messages the broker delivered on it, and messages acked on it.
async Task<Dictionary<string, (long Delivered, long Acked)>> ChannelStatsAsync()
{
    var result = new Dictionary<string, (long, long)>();
    foreach (var consumer in await QueueConsumersAsync())
    {
        var channel = consumer.GetProperty("channel_details").GetProperty("name").GetString()!;
        var stats = await http.GetFromJsonAsync<JsonElement>($"/api/channels/{Uri.EscapeDataString(channel)}");
        long Read(string name) => stats.TryGetProperty("message_stats", out var m) && m.TryGetProperty(name, out var v) ? v.GetInt64() : 0;
        result[channel] = (Read("deliver_get"), Read("ack"));
    }

    return result;
}

async Task<long> QueueDepthAsync(string queue)
{
    var response = await http.GetAsync($"/api/queues/%2F/{Uri.EscapeDataString(queue)}");
    if (!response.IsSuccessStatusCode)
    {
        return 0;
    }

    var q = await response.Content.ReadFromJsonAsync<JsonElement>();
    return q.TryGetProperty("messages", out var m) ? m.GetInt64() : 0;
}

async Task<long> QueueUnackedAsync(string queue)
{
    var q = await http.GetFromJsonAsync<JsonElement>($"/api/queues/%2F/{Uri.EscapeDataString(queue)}");
    return q.TryGetProperty("messages_unacknowledged", out var m) ? m.GetInt64() : 0;
}

async Task WatchConsumersAsync(TimeSpan duration)
{
    var last = -1;
    var until = DateTime.UtcNow + duration;
    while (DateTime.UtcNow < until)
    {
        var count = await ConsumersAsync();
        if (count != last)
        {
            Console.WriteLine($"{DateTime.Now:HH:mm:ss} consumers on '{QueueName}': {count}");
            last = count;
        }

        await Task.Delay(1000);
    }
}

await host.StartAsync();
var bus = host.Services.GetRequiredService<IMessageBus>();

Console.WriteLine($"Wolverine {typeof(IMessageBus).Assembly.GetName().Version}, ListenerCount = {listenerCount}, mode = {mode}, retries = {(retry ? "on" : "off")}, PauseTime = {pauseTime.TotalSeconds}s");

// The broker's consumer list catches up a moment after the listener starts.
for (var i = 0; i < 30 && await ConsumersAsync() < listenerCount; i++)
{
    await Task.Delay(1000);
}

Console.WriteLine($"{DateTime.Now:HH:mm:ss} consumers on '{QueueName}': {await ConsumersAsync()} (expected {listenerCount})");

Console.WriteLine($"{DateTime.Now:HH:mm:ss} publishing {FailingMessages} failing messages to trip the circuit breaker");
for (var i = 0; i < FailingMessages; i++)
{
    await bus.PublishAsync(new Work(-1 - i, Fail: true));
}

// Long enough for two pause/restart cycles.
await WatchConsumersAsync(pauseTime * 3 + 20.Seconds());

// Broker statistics refresh every 5 seconds, so take the "before" snapshot after a quiet period.
var consumersAfter = await ConsumersAsync();
var before = await ChannelStatsAsync();
var deadLettersBefore = await QueueDepthAsync("wolverine-dead-letter-queue");
var receivedBefore = wolverineLog.Count(IsReceipt);

Console.WriteLine($"{DateTime.Now:HH:mm:ss} publishing {GoodMessages} good messages");
for (var i = 0; i < GoodMessages; i++)
{
    await bus.PublishAsync(new Work(i, Fail: false));
}

await Task.Delay(15.Seconds());
var handled = WorkHandler.Handled.Keys.Count(id => id >= 0);
var after = await ChannelStatsAsync();
var received = wolverineLog.Count(IsReceipt) - receivedBefore;

Console.WriteLine();
Console.WriteLine("Per consumer, for the good messages (from the broker's channel statistics):");
foreach (var (channel, (delivered, acked)) in after.OrderBy(x => x.Key))
{
    var (d0, a0) = before.GetValueOrDefault(channel);
    Console.WriteLine($"  {channel,-45} delivered {delivered - d0,3}, acked {acked - a0,3}");
}

Console.WriteLine();
Console.WriteLine($"Consumers on the queue:          {consumersAfter} (expected {listenerCount})");
Console.WriteLine($"Good messages handled:           {handled} of {GoodMessages}");
Console.WriteLine($"Receipts logged by Wolverine:    {received} (\"Received message\" debug lines)");
Console.WriteLine($"Left on the queue:               {await QueueDepthAsync(QueueName)}");
Console.WriteLine($"Unacked on the queue:            {await QueueUnackedAsync(QueueName)}");
Console.WriteLine($"New dead letters:                {await QueueDepthAsync("wolverine-dead-letter-queue") - deadLettersBefore}");
var leftOnQueue = await QueueDepthAsync(QueueName);
Console.WriteLine(consumersAfter < listenerCount
    ? "Inconclusive: the listener was paused when the good messages were sent (they are still on the queue)"
    : consumersAfter > listenerCount || handled + leftOnQueue < GoodMessages
        ? $"BUG REPRODUCED: {consumersAfter - listenerCount} leaked consumer(s), {GoodMessages - handled - leftOnQueue} message(s) acked but never handled"
        : "Not reproduced on this run");

await host.StopAsync();
await File.WriteAllLinesAsync("repro-debug.log", wolverineLog.Lines);

static bool IsReceipt(string line) => line.Contains("Received message from rabbitmq://queue/" + "ghost-consumer-repro");

public record Work(int Id, bool Fail);

public static class WorkHandler
{
    public static readonly ConcurrentDictionary<int, byte> Handled = new();

    public static void Handle(Work work)
    {
        if (work.Fail)
        {
            throw new InvalidOperationException($"Failing on purpose ({work.Id})");
        }

        Handled.TryAdd(work.Id, 0);
    }
}

// Keeps every Wolverine log line in memory, so the run can count receipts and write repro-debug.log.
public sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<string> _lines = new();

    public IEnumerable<string> Lines => _lines;

    public int Count(Func<string, bool> predicate) => _lines.Count(predicate);

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(categoryName, _lines);

    public void Dispose()
    {
    }

    private sealed class CapturingLogger(string category, ConcurrentQueue<string> lines) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lines.Enqueue($"{DateTime.Now:HH:mm:ss.fff} {logLevel,-11} {category}: {formatter(state, exception)}");
        }
    }
}
