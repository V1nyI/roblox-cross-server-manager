# CrossServerManager v1.1.0 - Now with Offline Simulation & Virtual Servers

A Roblox module for **advanced cross-server messaging**, supporting reliable message delivery, retries, deduplication, replay, throttling, custom monitoring, and more, now with a **full offline simulation mode** so you can develop and test without needing live Roblox servers.

link: https://github.com/V1nyI/roblox-cross-server-manager

## **What's New in v1.1.0**

* **Offline Mode** - Test your game without Roblox backend services
* **Virtual Servers** - Create simulated servers for local multi-server testing in Studio
* **Service Limit Simulation** - Configure quotas for MessagingService, MemoryStoreService, and DataStoreService to match Roblox limits
* **Limit Statistics** - Check usage counters in real time with `GetLimitStats()`
* **Offline Queue & Replay** - Queue processing and message replay now work fully in offline mode
* **Targeted Replay** - In offline mode, replay missed messages to a specific simulated server
* **Backward Compatible** - All online mode features from v1.0.5 remain intact

## **Feature Comparison**

|Feature|v1.0.5 (Old)|v1.1.0 (New)|
| --- | --- | --- |
|Modes|Online only|Online + Offline|
|Virtual Servers|❌|✅|
|Limit Simulation|❌|✅|
|Limit Stats|❌|✅|
|Offline Queue|❌|✅|
|Offline Replay|❌|✅|
|Targeted Replay|❌|✅|
|Backwards Compatibility|✅|✅|

# Offline Mode Example
```lua
local CSM = require(path.to.CrossServerManager)
CSM:SetMode("offline")

local serverA = CSM:CreateVirtualSimulationServer("ServerA")
local serverB = CSM:CreateVirtualSimulationServer("ServerB")

serverB:Subscribe("Chat", function(payload)
    print("[ServerB] Received:", payload)
end)

serverA:Publish("Chat", "Hello from A!")
```
---

## Features

- Reliable cross-server messaging
- Automatic retry with exponential backoff
- Deduplication for safe message processing
- Message replay for new/late servers
- Per-message-type throttling & configuration
- Dead-letter queue on repeated failure
- Custom monitoring event hooks
- Flush pending messages on server shutdown
- Flexible subscription control (pause, resume, unsubscribe)
- Version check and update notification
- Rate limit awareness with degradation

---

## Installation

1. Copy `CrossServerManager.lua` into your Roblox project (ideally in `ServerScriptService`).
2. Require it in your server-side script:
   ```lua
   local CrossServerManager = require(path.to.CrossServerManager)
   ```

---

## Initialization / Starting the Module

Before using the module, you should start its internal processes.  
**Call `:Start()` as early as possible on the server (e.g., at the top of your main server script):**

```lua
local CrossServerManager = require(path.to.CrossServerManager)
CrossServerManager:Start() -- Initialize
```

You only need to call `:Start()` once per server, Set Variable `Debug` to Enable `_log()`

---

## Basic Usage

### note: Set localPublish to true to publish the message only locally on the current server. This is useful for testing or when you want to avoid sending messages across servers.


```lua
-- Subscribe to a topic
local subscription = CrossServerManager:Subscribe("MyTopic", function(payload, uuid, seq, messageType)
    print("Received:", payload)
end)

-- Publish a message
local uuid, success, err = CrossServerManager:Publish("MyTopic", {data = 123}, "default", 60)

-- Pause, resume, or unsubscribe from a topic
subscription:Pause()
subscription:Resume()
subscription:Unsubscribe()
```

---

## Replay Missed Messages

### Replay is one of the most powerful features of this module. If your server was offline or you want to "catch up" on messages sent before joining,  

#### **use `ReplayMissedMessages` to process messages from a given point in time:**

```lua
-- Replay all messages for "MyTopic" sent since a specific timestamp (e.g., last hour)
local oneHourAgo = os.time() - 3600
CrossServerManager:ReplayMissedMessages("MyTopic", oneHourAgo)
```

- ### This will fetch and deliver up to the last 40 messages for the topic that were sent since the given timestamp.
- #### Note: Messages must have been published with messageRetentionTime > 0 and not expired to be eligible for replay.
- Deduplication ensures you won't process the same message twice.
- Useful when a new server starts or for recovering missed events.

---

## Advanced Usage

### Monitoring Events

```lua
CrossServerManager:MonitoringOn("onMessageSent", function(uuid, topic, payload, seq, messageType)
    print("Message sent:", uuid)
end)
```

---

## **New API Additions**

* `SetMode(mode)` - `"online"` or `"offline"`
* `CreateVirtualSimulationServer(name)` - Create a simulated server in offline mode
* `SetLimitSimulation(config)` - Enable/disable limit simulation per service
* `GetLimitStats(service)` - View current usage stats for simulated limits

## **Full API Overview**

* `:Start()` - Initializes the module
* `:Subscribe(topic, callback)` - Subscribe to messages on a topic
* `:Publish(topic, payload, messageType, messageRetentionTime, localPublish)` - Publish a message
* `:ReplayMissedMessages(topic, sinceTimestamp, [targetServer])` - Replay recent messages
* `:BulkPublish(messages, localBulkPublish)` - Publish multiple messages
* `:FlushPendingMessages()` - Immediately flush all pending messages
* `:MonitoringOn(eventName, callback)` - Listen for monitoring events
* `:Unsubscribe(topic, id)` - Unsubscribe a specific callback
* `:GetServerId()` - Get the current server ID
* `:SetDebugMode(enabled)` - Enable or disable debug logs
* **Offline mode only:** `SetMode`, `CreateVirtualSimulationServer`, `SetLimitSimulation`, `GetLimitStats`
---

---

## **Update Logs – v1.1.0**

* Added offline mode with virtual server support
* Added simulated service quotas and statistics
* Extended queue and replay to work offline
* Added targeted replay for specific virtual servers
* Maintains full backwards compatibility with v1.0.5
---

## License

MIT - see [LICENSE](https://github.com/V1nyI/roblox-cross-server-manager/blob/main/LICENSE) for details.

