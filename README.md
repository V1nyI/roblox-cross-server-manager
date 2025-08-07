# CrossServerManager

A Roblox module for advanced cross-server messaging, Supports reliable message delivery, retries, deduplication, replay, throttling, custom monitoring, using Roblox’s MessagingService and MemoryStoreService APIs.

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

You only need to call `:Start()` once per server.

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

Replay is one of the most powerful features of this module.
If your server was offline or you want to "catch up" on messages sent before joining,  
**use `ReplayMissedMessages` to process messages from a given point in time:**

```lua
-- Replay all messages for "MyTopic" sent since a specific timestamp (e.g., last hour)
local oneHourAgo = os.time() - 3600
CrossServerManager:ReplayMissedMessages("MyTopic", oneHourAgo)
```

- This will fetch and deliver up to the last 40 messages for the topic that were sent since the given timestamp.
- Deduplication ensures you won't process the same message twice.
- Useful when a new server starts or for recovering missed events.
- Note: Messages must have been published with messageRetentionTime > 0 and not expired to be eligible for replay.

---

## Advanced Usage

### Monitoring Events

```lua
CrossServerManager:MonitoringOn("onMessageSent", function(uuid, topic, payload, seq, messageType)
    print("Message sent:", uuid)
end)
```

---

## API Overview

- `:Start()` – Initializes the module (run this once when your server starts).
- `:Subscribe(topic, callback)` – Subscribe to messages on a topic.
- `:Publish(topic, payload, messageType, messageRetentionTime)` – Publish a message. If you provide a `messageRetentionTime` (in seconds), the message will be retained in MemoryStore for replay and auto-processing.
- `:ReplayMissedMessages(topic, sinceTimestamp)` – Replay recent messages (see above).
- `:FlushPendingMessages()` – Immediately flush all pending messages (call on shutdown).
- `:MonitoringOn(eventName, callback)` – Listen for internal monitoring events.
- `:Unsubscribe(topic, id)` – Unsubscribe a specific callback from a topic.
- `:BulkPublish(messages: {{topic: string, payload: any, messageType: string, messageRetentionTime: number, localPublish: boolean})` – Publish Multiple Messages
---

---

## Update logs, Version: "v1.0.5"
- `:BulkPublish(messages: {{topic: string, payload: any, messageType: string, messageRetentionTime: number, localPublish: boolean})` – Publish Multiple Messages
## BulkPublish
- ### Publishes all messages atomically — if one fails, none are sent.
- #### Supports up to 100 messages in a single batch
- Supports both MemoryStore-based retention and local-only publish.
- Bypasses message queue if flagged for local delivery only.

## Example Usage:
```
local messages = {
	{
		topic = "Topic1",
		payload = {
			userId = 1,
			score = 100
		}
	},
	{
		topic = "Topic2",
		payload = {
			event = "Rain",
			time = os.time(),
            AdminMessage = "Rain has started! Let your friends know, they have 60 seconds to join and catch the event!"
		},
        messageRetentionTime = 60 -- Optional: make it replayable for 60s
	}
}

CrossServerManager:BulkPublish(messages)
```
---

## License

MIT — see [LICENSE](LICENSE) for details.

---

## Author

Created by [Vinyl_Module](https://www.roblox.com/users/766671012/profile) (Roblox)  
GitHub: [V1nyI](https://github.com/V1nyI)
