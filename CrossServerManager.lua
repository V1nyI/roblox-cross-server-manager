--// Services
local MessagingService = game:GetService("MessagingService")
local MemoryStoreService = game:GetService("MemoryStoreService")
local RunService = game:GetService("RunService")
local HttpService = game:GetService("HttpService")
local Players = game:GetService("Players")

--// Debug
local _Debug = true

--// Constants
local MEMORY_STORE_MAP_NAME = "CrossServerMessages"
local QUEUE_MEMORY_STORE_NAME = "CrossServerMessageQueue"
local MEMORY_STORE_EXPIRY = 0
local DEDUPE_CACHE_SIZE = 1000 -- max number of UUIDs in dedupe cache before cleanup
local DEFAULT_RETRY_BACKOFF = {base = 1, max = 30, multiplier = 2}
local MAX_RECENT_MESSAGES = 40 -- how many messages to replay to new servers
local QUEUE_LIMIT = 40
local QUEUE_PROCCESS_INTERVAL = 0.5 -- seconds
local MAX_RU_CALLS_PER_MINUTE = 120
local RU_COOLDOWN = 60 -- seconds
local THROTTLE_BACKOFF_SECONDS = 2  -- seconds

export type UUID = string
export type Topic = string
export type ServerId = string
export type MessageType = string

export type MessagePayload = any

export type Message = {
	uuid: UUID,
	topic: Topic,
	payload: MessagePayload,
	seq: number,
	timestamp: number,
	messageType: MessageType,
}

export type RetryPolicy = {
	base: number,
	max: number,
	multiplier: number,
}

export type PendingMessage = {
	uuid: UUID,
	topic: Topic,
	payload: MessagePayload,
	seq: number,
	retryCount: number,
	nextRetryTime: number,
	retryPolicy: RetryPolicy,
}

export type SubscriberCallback = (payload: MessagePayload, uuid: UUID, seq: number, messageType: MessageType) -> ()

export type Subscribers = {
	[Topic]: {[string]: {callback: SubscriberCallback, active: boolean}}
}

export type ReceivedUUIDs = {
	[UUID]: number, -- timestamp when received
}

export type RecentMessages = {Message}

export type LastSeqPerTopic = {
	[Topic]: number,
}

export type AckEntry = {
	servers: {[ServerId]: true},
	requiredAckCount: number?,
}

export type LastAckPerMessage = {
	[UUID]: AckEntry,
}

export type MessageTypeConfigEntry = {
	retryPolicy: RetryPolicy?,
	throttleLimit: number?,
	priority: number?,
}

export type MessageTypeConfig = {
	[MessageType]: MessageTypeConfigEntry,
}

export type ServerIdType = string

--// Internal State
local _receivedUUIDs: ReceivedUUIDs = {}
local _recentMessages: RecentMessages = {}
local _pendingMessages: {PendingMessage} = {}
local _subscribers: Subscribers = {}
local _serverId: ServerIdType = game.JobId
local _lastSeqPerTopic: LastSeqPerTopic = {}
local _lastAckPerMessage: LastAckPerMessage = {}
local _messageTypeConfig: MessageTypeConfig = {}
local _activeSubscriptions = {}
local _messageTypeCounters = {}
local _initialized = false
local _queueProcessorRunning = false
local _lastRUReset = os.time()
local _ruCallCount = 0
local _isThrottled = false
local _lastThrottleTime = 0
local THROTTLE_WINDOW = 1

--// Monitoring and Callbacks
local Monitoring = {}
Monitoring._listeners = {
	onMessageSent = {},
	onMessageReceived = {},
	onMessageDeduped = {},
	onMessageFailed = {},
	onMessageRetried = {},
	onDeadLetter = {},
}

--// Private Variables
local _CURRENT_VERSION = "v1.0.5"
local _VERSION_URL = "https://raw.githubusercontent.com/V1nyI/roblox-cross-server-manager/refs/heads/main/Version.txt"

--// Utilities
local function _log(level, ...)
	if _Debug == false then return end

	local prefix = "[CrossServerManager]:"
	if level == "log" then
		print(prefix, ...)
	elseif level == "warn" then
		warn(prefix, ...)
	elseif level == "error" then
		error(prefix, ...)
	end
end

local function _checkForUpdate()
	local success, latestVersion = pcall(function()
		return HttpService:GetAsync(_VERSION_URL)
	end)
	if success and latestVersion then
		latestVersion = latestVersion:match("^%s*(.-)%s*$")
		if latestVersion ~= _CURRENT_VERSION then
			warn("[CrossServerManager]: New version available:", latestVersion)
		end
	else
		_log("warn", "Failed to check for updates:", latestVersion)
	end
end

local function _generateUUID()
	return HttpService:GenerateGUID(false)
end

local function _invokeListeners(listeners, ...)
	for _, callback in ipairs(listeners) do
		local ok, err = pcall(callback, ...)
		if not ok then
			_log("warn", "Monitoring listener error:", err)
		end
	end
end

local function _dedupeAdd(uuid)
	_receivedUUIDs[uuid] = os.time()
	if #_receivedUUIDs > DEDUPE_CACHE_SIZE then
		local timestamps = {}
		for id, time in pairs(_receivedUUIDs) do
			table.insert(timestamps, {id = id, time = time})
		end
		table.sort(timestamps, function(a,b) return a.time < b.time end)
		for i = 1, math.floor(#timestamps/2) do
			_receivedUUIDs[timestamps[i].id] = nil
		end
	end
end

local function _dedupeCheck(uuid)
	return _receivedUUIDs[uuid] ~= nil
end

local function _addRecentMessage(message)
	table.insert(_recentMessages, message)
	if #_recentMessages > MAX_RECENT_MESSAGES then
		table.remove(_recentMessages, 1)
	end
end

local function _scheduleRetry(message)
	local retryCount = message.retryCount or 0
	local policy = message.retryPolicy or DEFAULT_RETRY_BACKOFF
	local backoff = math.min(policy.base * (policy.multiplier ^ retryCount), policy.max)
	message.retryCount = retryCount + 1
	message.nextRetryTime = os.time() + backoff
	table.insert(_pendingMessages, message)
end

local function _removePending(uuid)
	for i, msg in ipairs(_pendingMessages) do
		if msg.uuid == uuid then
			table.remove(_pendingMessages, i)
			break
		end
	end
end

local function _canProcessMessage(messageType)
	local now = os.time()
	local counter = _messageTypeCounters[messageType]
	if not counter then
		_messageTypeCounters[messageType] = {count = 1, windowStart = now}
		return true
	end

	if now - counter.windowStart >= THROTTLE_WINDOW then
		counter.count = 1
		counter.windowStart = now
		return true
	end

	if counter.count < (_messageTypeConfig[messageType] and _messageTypeConfig[messageType].throttleLimit or math.huge) then
		counter.count = counter.count + 1
		return true
	end

	return false
end

local function _RUThrottleCheck()
	local now = os.time()

	if now - _lastRUReset >= RU_COOLDOWN then
		_lastRUReset = now
		_ruCallCount = 0
	end

	if _isThrottled and (tick() - _lastThrottleTime) < THROTTLE_BACKOFF_SECONDS then
		return false, "RecentlyThrottled"
	end

	if _ruCallCount >= MAX_RU_CALLS_PER_MINUTE then
		return false, "RateLimit"
	end

	_ruCallCount += 1
	return true
end

--// Messaging API

local CrossServerManager = {}

--[[
    Subscribe to a topic
    
	@return SubscriptionHandle
]]
function CrossServerManager:Subscribe(topic: Topic, callback: SubscriberCallback)
	assert(type(callback) == "function", "Callback must be function")

	local id = HttpService:GenerateGUID(false)

	_subscribers[topic] = _subscribers[topic] or {}
	_subscribers[topic][id] = {
		callback = callback,
		active = true,
	}

	if not _activeSubscriptions[topic] then
		local ok, subscriptionOrErr = pcall(function()
			return MessagingService:SubscribeAsync(topic, _onMessageReceived)
		end)

		if ok and subscriptionOrErr then
			_activeSubscriptions[topic] = subscriptionOrErr
			_log("log", "Subscribed to", topic)
		else
			_log("warn", "Failed to subscribe to", topic, subscriptionOrErr)
		end
	end

	local SubscriptionHandle = {}
	SubscriptionHandle.__index = SubscriptionHandle

	function SubscriptionHandle:Pause()
		local entry = _subscribers[topic] and _subscribers[topic][id]
		if entry then
			entry.active = false
		end
	end

	function SubscriptionHandle:Resume()
		local entry = _subscribers[topic] and _subscribers[topic][id]
		if entry then
			entry.active = true
		end
	end

	function SubscriptionHandle:IsPaused()
		local entry = _subscribers[topic] and _subscribers[topic][id]
		return entry and not entry.active or false
	end

	function SubscriptionHandle:PauseFor(seconds)
		assert(type(seconds) == "number" and seconds > 0, "PauseFor requires a positive number of seconds")
		self:Pause()
		task.delay(seconds, function()
			if _subscribers[topic] and _subscribers[topic][id] then
				self:Resume()
			end
		end)
	end

	function SubscriptionHandle:Unsubscribe()
		CrossServerManager:Unsubscribe(topic, id)
		setmetatable(self, nil)
		for k in pairs(self) do self[k] = nil end
	end

	return setmetatable({}, SubscriptionHandle)
end

--[[
	Update the retention time for a message
]]
function CrossServerManager:UpdateRetentionTime(uuid: string, newRetentionTime: number)
	local map = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME)
	if not map then
		return false, "MemoryStore unavailable"
	end

	local success, raw = pcall(function()
		return map:GetAsync(uuid)
	end)

	if not success or not raw then
		return false, "Message not found"
	end

	local ok, message = pcall(function()
		return HttpService:JSONDecode(raw)
	end)

	if not ok or type(message) ~= "table" then
		return false, "Malformed message data"
	end

	if message.status == "cancelled" or message.status == "published" then
		return false, "Cannot update retention for finalized message"
	end

	message.retentionTime = newRetentionTime

	local encoded = HttpService:JSONEncode(message)

	local setSuccess, setErr = pcall(function()
		map:SetAsync(uuid, encoded, newRetentionTime)
	end)

	if not setSuccess then
		return false, "Failed to update retention: ".. tostring(setErr)
	end

	return true
end

--[[
	Cancel a message by UUID
]]
function CrossServerManager:CancelPublish(uuid: string)
	local map = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME)
	if not map then
		return false, "MemoryStore unavailable"
	end

	local success, raw = pcall(function()
		return map:GetAsync(uuid)
	end)

	if not success or not raw then
		return false, "Message not found"
	end

	local ok, message = pcall(function()
		return HttpService:JSONDecode(raw)
	end)

	if not ok or type(message) ~= "table" then
		return false, "Malformed message data"
	end

	message.status = "cancelled"

	local encoded = HttpService:JSONEncode(message)

	local setSuccess, setErr = pcall(function()
		map:SetAsync(uuid, encoded, 10)
	end)

	if not setSuccess then
		return false, "Failed to cancel message: ".. tostring(setErr)
	end

	return true
end

--[[
	Cancel all publishes
]]
function CrossServerManager:CancelAllPublishes()
	local map = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME)
	local queueMap = MemoryStoreService:GetSortedMap(QUEUE_MEMORY_STORE_NAME)

	if not map or not queueMap then
		return false, "MemoryStore unavailable"
	end

	local function cancelMessagesInMap(targetMap, shouldRemove)
		local cancelled = 0

		local success, entries = pcall(function()
			return targetMap:GetRangeAsync(Enum.SortDirection.Ascending, 100)
		end)

		if not success then
			_log("warn", "Failed to retrieve messages from map.")
			return 0
		end

		for _, entry in ipairs(entries) do
			if shouldRemove then
				local okRemove, errRemove = pcall(function()
					targetMap:RemoveAsync(entry.key)
				end)

				if okRemove then
					cancelled += 1
				else
					_log("warn", "Failed to remove queueMap message "..tostring(entry.key)..": "..tostring(errRemove))
				end
			else
				local raw = nil
				local okGet, errGet = pcall(function()
					raw = targetMap:GetAsync(entry.key)
				end)

				if okGet and raw then
					local okDecode, message = pcall(function()
						return HttpService:JSONDecode(raw)
					end)

					if okDecode and type(message) == "table" and message.status ~= "cancelled" then
						message.status = "cancelled"
						local encoded = HttpService:JSONEncode(message)

						local okSet, errSet = pcall(function()
							targetMap:SetAsync(entry.key, encoded, 10)
						end)

						if okSet then
							cancelled += 1
						else
							_log("warn", "Failed to cancel message "..tostring(entry.key)..": "..tostring(errSet))
						end
					end
				else
					_log("warn", "Failed to get message "..tostring(entry.key)..": "..tostring(errGet))
				end
			end
		end

		return cancelled
	end

	local cancelCountMap = cancelMessagesInMap(map, false)
	local cancelCountQueue = cancelMessagesInMap(queueMap, true)
	local totalCancelled = cancelCountMap + cancelCountQueue

	return true, ("Cancelled %d messages (map: %d, queueMap: %d)"):format(totalCancelled, cancelCountMap, cancelCountQueue)
end

--[[
	Unsubscribe from a topic
]]
function CrossServerManager:Unsubscribe(topic: Topic, id: string)
	local topicSubs = _subscribers[topic]
	if not topicSubs then return end

	topicSubs[id] = nil

	if next(topicSubs) == nil then
		_subscribers[topic] = nil

		local subscription = _activeSubscriptions[topic]
		if subscription then
			local success, err = pcall(function()
				if typeof(subscription) == "RBXScriptConnection" then
					subscription:Disconnect()
				else
					subscription:Unsubscribe()
				end
			end)

			if not success then
				_log("warn", "Failed to unsubscribe from MessagingService topic:", topic, err)
			else
				_log("log", "Unsubscribed from MessagingService topic:", topic)
			end

			_activeSubscriptions[topic] = nil
		end
	end
end

local function _fireSubscribers(topic, uuid, payload, seq, messageType)
	local callbackMap = _subscribers[topic]
	if not callbackMap then return end
	for _, entry in pairs(callbackMap) do
		if entry.active then
			local ok, err = pcall(entry.callback, payload, uuid, seq, messageType)
			if not ok then
				_log("warn", "Subscriber callback error:", err)
			end
		end
	end
end

--[[
	Process the queue and publish messages
	
	@note This function is called by the main loop and should not be called directly.
]]
function CrossServerManager:_Proccess_Queue(topic: string, payload: any, messageType: string, messageRetentionTime: number, localPublish: boolean)
	local QUEUE_MEMORY_STORE = MemoryStoreService:GetSortedMap(QUEUE_MEMORY_STORE_NAME)

	local okRU, reason = _RUThrottleCheck()
	if not okRU then
		_log("warn", "Skipping due to "..reason.." Cooldown for 30 seconds...")
		task.wait(30)
		return nil, false, reason
	end

	local entries
	local success, result = pcall(function()
		return QUEUE_MEMORY_STORE:GetRangeAsync(Enum.SortDirection.Ascending, 100)
	end)

	if not success then
		if tostring(result):find("RequestThrottled") then
			_isThrottled = true
			_lastThrottleTime = tick()
			_log("warn", "MemoryStore throttled, backing off: "..tostring(result))
			return nil, false, "RequestThrottled"
		end

		_log("warn", "Failed to fetch queue entries: "..tostring(result))
		return nil, false, "QueueFetchFailed"
	end

	entries = result

	_isThrottled = false

	if #entries >= QUEUE_LIMIT then
		_log("warn", "Queue is full, skipping message")
		task.wait(10)
		return nil, false, "QueueFull"
	end

	local totalSize = 0
	local payloadSize = #HttpService:JSONEncode(payload)
	for _, entry in ipairs(entries) do
		totalSize += #entry.value
	end

	if totalSize + payloadSize > 20000 then
		_log("warn", "Queue size limit reached, skipping message")
		return nil, false, "QueueSizeLimit"
	end

	local uuid = _generateUUID()
	local now = os.time()
	local queueKey = tostring(now).."_"..uuid

	local message = {
		uuid = uuid,
		topic = topic,
		payload = payload,
		seq = now,
		timestamp = now,
		messageType = messageType,
		localPublish = localPublish,
		retentionTime = messageRetentionTime,
	}

	local encoded = HttpService:JSONEncode(message)
	local success, err = pcall(function()
		QUEUE_MEMORY_STORE:SetAsync(queueKey, encoded, 120)
	end)

	if not success then
		if tostring(err):find("RequestThrottled") then
			_isThrottled = true
			_lastThrottleTime = tick()
			_log("warn", "SetAsync throttled, backing off: "..tostring(err))
			return nil, false, "RequestThrottled"
		end

		_log("warn", "Failed to enqueue message: "..tostring(err))
		return nil, false, "QueueEnqueueFailed"
	end

	if not _queueProcessorRunning then
		_queueProcessorRunning = true
		task.spawn(function()
			while true do
				local okRU, reason = _RUThrottleCheck()
				if not okRU then
					task.wait(1)
					continue
				end

				local ok, entries = pcall(function()
					return QUEUE_MEMORY_STORE:GetRangeAsync(Enum.SortDirection.Ascending, 1)
				end)

				if not ok then
					if tostring(entries):find("RequestThrottled") then
						_isThrottled = true
						_lastThrottleTime = tick()
						_log("warn", "Processor throttled, backing off.")
						task.wait(1)
						continue
					end
				end

				_isThrottled = false

				if entries and #entries > 0 then
					local entry = entries[1]
					local raw = nil
					local okGet = pcall(function()
						raw = QUEUE_MEMORY_STORE:GetAsync(entry.key)
					end)

					if okGet and raw then
						local okDecode, msg = pcall(function()
							return HttpService:JSONDecode(raw)
						end)
						
						if okDecode and type(msg) == "table" then
							CrossServerManager:Publish(msg.topic, msg.payload, msg.messageType, msg.retentionTime, true, true)
						end
						
						pcall(function()
							QUEUE_MEMORY_STORE:RemoveAsync(entry.key)
						end)
					end
				end

				task.wait(QUEUE_PROCCESS_INTERVAL)
			end
		end)
	end

	return uuid, true
end

local function validatePayload(payload)
	local payloadType = typeof(payload)
	
	if payloadType == "string" or payloadType == "number" or payloadType == "boolean" or payload == nil then
		return true
	end
	
	if payloadType == "Instance" then
		return false, "Payload contains Roblox Instance, which is not allowed"
	end
	
	if payloadType == "table" then
		for key, value in pairs(payload) do
			local keyType = typeof(key)
			if keyType ~= "string" and keyType ~= "number" then
				return false, ("Invalid key type in payload: %s"):format(keyType)
			end
			
			local ok, err = validatePayload(value)
			if not ok then
				return false, err
			end
		end
		return true
	end
	
	return false, ("Unsupported payload type: %s"):format(payloadType)
end

--[[
	Publish a message to a topic
	
	@note do not use _BypassQueue parameter, it is for internal use only.
]]
function CrossServerManager:Publish(topic: string, payload: any, messageType: string, messageRetentionTime: number, localPublish: boolean, _BypassQueue: boolean)
	messageType = messageType or "default"
	if localPublish == nil then
		localPublish = false
	end
	
	local ok, err = validatePayload(payload)
	if not ok then
		_log("warn", "Publish: Invalid payload for topic", topic, err)
		return nil, false, "InvalidPayload: "..err
	end
	
	if not _canProcessMessage(messageType) then
		_log("warn", "Throttled message publish for type", messageType)
		return nil, false, "Throttled"
	end
	
	local config = _messageTypeConfig[messageType] or {}
	local retryPolicy = config.retryPolicy or DEFAULT_RETRY_BACKOFF
	
	local now = os.time()
	_lastSeqPerTopic[topic] = (_lastSeqPerTopic[topic] or 0) + 1
	local seq = _lastSeqPerTopic[topic]
	
	local uuid = _generateUUID()
	local message = {
		uuid = uuid,
		topic = topic,
		payload = payload,
		seq = seq,
		timestamp = now,
		messageType = messageType,
		retryCount = 0,
		retryPolicy = retryPolicy,
		serverId = _serverId,
	}
	
	if not _BypassQueue and not localPublish then
		return self:_Proccess_Queue(topic, payload, messageType, messageRetentionTime, localPublish)
	end
	
	if localPublish then
		if _dedupeCheck(uuid) then
			_log("warn", "Duplicate local publish UUID detected, ignoring:", uuid)
			return nil, false, "Duplicate"
		end
		
		local encodedMessage = HttpService:JSONEncode(message)
		local messageSize = #encodedMessage
		if messageSize >= 990 then
			_log("warn", "Message too large for local publish ("..messageSize.." bytes), rejecting:", topic)
			return nil, false, "MessageTooLarge"
		end
		
		_dedupeAdd(uuid)
		_fireSubscribers(topic, uuid, payload, seq, messageType)
		_invokeListeners(Monitoring._listeners.onMessageSent, uuid, topic, payload, seq, messageType)

		_lastAckPerMessage[uuid] = {
			servers = {[_serverId] = true},
			requiredAckCount = nil,
		}
		
		return uuid, true
	end
	
	local useMemoryStore = (type(messageRetentionTime) == "number" and messageRetentionTime > 0)
	if not useMemoryStore then
		messageRetentionTime = MEMORY_STORE_EXPIRY
	end
	
	local MessageSize = #HttpService:JSONEncode(message)

	if MessageSize >= 990 then
		_log("warn", "Message too large for MessagingService ("..MessageSize.." bytes), using MemoryStore for topic:", topic)
		return
	end
	
	if useMemoryStore then
		local timestampKey = tostring(now).."_"..uuid
		local map = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME)
		local encoded = HttpService:JSONEncode(message)
		local success, err = pcall(function()
			map:SetAsync(timestampKey, encoded, messageRetentionTime)
		end)
		
		if not success then
			_log("warn", "MemoryStore set failed:", err)
		end
		
		_addRecentMessage(message)
	end
	
	local successMs, errMs = pcall(function()
		MessagingService:PublishAsync(topic, HttpService:JSONEncode(message))
	end)

	if successMs then
		_lastAckPerMessage[uuid] = {
			servers = {[_serverId] = true},
			requiredAckCount = nil,
		}
		_invokeListeners(Monitoring._listeners.onMessageSent, uuid, topic, payload, seq, messageType)
		return uuid, true
	else
		_log("warn", "MessagingService publish failed:", errMs)
		_invokeListeners(Monitoring._listeners.onMessageFailed, uuid, topic, errMs)
		_scheduleRetry(message)
		return uuid, false, errMs
	end
end

--[[
	Publish Multiple Messages
]]
function CrossServerManager:BulkPublish(messages: {{topic: string, payload: any, messageType: string, messageRetentionTime: number, localPublish: boolean}}, localBulkPublish: boolean)
	assert(type(messages) == "table" and #messages > 0, "BulkPublish expects a non-empty array of message entries")
	
	if #messages > 100 then
		_log("warn", "BulkPublish: Exceeded max batch size of 100 messages")
		return false, "Exceeded max batch size of 100 messages"
	end
	
	if localBulkPublish == true then
		for _, v in ipairs(messages) do
			v.localPublish = true
		end
	end
	
	local QUEUE_MEMORY_STORE = MemoryStoreService:GetSortedMap(QUEUE_MEMORY_STORE_NAME)
	local queuedEntries = {}
	
	for index, msg in ipairs(messages) do
		if type(msg) ~= "table" then
			_log("warn", "BulkPublish: Invalid message at index", index)
			return false, "Invalid message at index "..index
		end
		
		local topic = msg.topic
		local payload = msg.payload
		local messageType = msg.messageType or "default"
		local retentionTime = msg.messageRetentionTime or MEMORY_STORE_EXPIRY
		local localPublish = msg.localPublish or false

		if type(topic) ~= "string" then
			_log("warn", "BulkPublish: Invalid topic at index", index)
			return false, "Invalid topic at index "..index
		end

		local ok, err = validatePayload(payload)
		if not ok then
			_log("warn", "BulkPublish: Invalid payload at index", index, err)
			return false, "Invalid payload at index "..index..": "..err
		end

		local okEncode, encodedPayload = pcall(function()
			return HttpService:JSONEncode(payload)
		end)

		if not okEncode or not encodedPayload then
			_log("warn", "BulkPublish: Payload encoding failed at index", index)
			return false, "Invalid payload at index "..index
		end

		if #encodedPayload >= 990 then
			_log("warn", "BulkPublish: Payload too large at index", index)
			return false, "Payload too large at index "..index
		end

		local uuid = HttpService:GenerateGUID(false)
		local timestamp = os.time()
		local queueKey = tostring(timestamp).."_"..uuid

		local encodedMessage = HttpService:JSONEncode({
			topic = topic,
			payload = payload,
			messageType = messageType,
			retentionTime = retentionTime,
			uuid = uuid,
			timestamp = timestamp,
		})

		local success, err = pcall(function()
			QUEUE_MEMORY_STORE:SetAsync(queueKey, encodedMessage, retentionTime)
		end)

		if success then
			table.insert(queuedEntries, {
				queueKey = queueKey,
				topic = topic,
				payload = payload,
				messageType = messageType,
				retentionTime = retentionTime,
				localPublish = localPublish,
				uuid = uuid,
			})
		else
			_log("warn", "BulkPublish: Failed to queue at index", index, err)

			for _, entry in ipairs(queuedEntries) do
				task.wait(0.3)
				pcall(function()
					QUEUE_MEMORY_STORE:RemoveAsync(entry.queueKey)
				end)
			end

			return false, "Failed to queue message at index "..index..": "..tostring(err)
		end
	end

	task.spawn(function()
		for _, entry in ipairs(queuedEntries) do
			self:Publish(
				entry.topic,
				entry.payload,
				entry.messageType,
				entry.retentionTime,
				entry.localPublish,
				true
			)
			task.wait(0.1)
		end
	end)

	return true
end

local function _processMessage(message)
	if _dedupeCheck(message.uuid) then
		_invokeListeners(Monitoring._listeners.onMessageDeduped, message.uuid, message.topic)
		return
	end

	if message.serverId and message.serverId ~= _serverId then
		return
	end

	_dedupeAdd(message.uuid)
	_fireSubscribers(message.topic, message.uuid, message.payload, message.seq, message.messageType)
	local ackEntry = _lastAckPerMessage[message.uuid] or {servers = {}, requiredAckCount = nil}
	ackEntry.servers[_serverId] = true
	_lastAckPerMessage[message.uuid] = ackEntry
	_invokeListeners(Monitoring._listeners.onMessageReceived, message.uuid, message.topic, message.payload, message.seq, message.messageType)
end

function _onMessageReceived(publishedMessage)
	local raw = publishedMessage.Data
	local ok, message = pcall(function()
		return HttpService:JSONDecode(raw)
	end)

	if not ok or type(message) ~= "table" or not message.uuid then
		_log("warn", "Received malformed message via MessagingService")
		return
	end

	if not _subscribers[message.topic] then return end
	_processMessage(message)
end

local function _memoryStorePoller()
	local map
	local success, result = pcall(function()
		return MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME)
	end)

	if not success or not result then
		_log("warn", "MemoryStoreService unavailable. Skipping MemoryStore polling. Error:", result)
		return
	end

	map = result
	local lastChecked = 0

	while true do
		local now = os.time()
		if now - lastChecked >= 30 then
			local ok, entriesOrErr = pcall(function()
				return map:GetRangeAsync(Enum.SortDirection.Descending, 1)
			end)

			if ok and entriesOrErr then
				for _, entry in ipairs(entriesOrErr) do
					local val = nil
					local successGet, errGet = pcall(function()
						val = map:GetAsync(entry.key)
					end)

					if successGet and val then
						local ok2, message = pcall(function()
							return HttpService:JSONDecode(val)
						end)
						if ok2 and type(message) == "table" and message.uuid then
							if not _dedupeCheck(message.uuid) then
								_processMessage(message)
							end
						end
					end
				end
			else
				_log("warn", "Failed to get keys from MemoryStore. Error:", entriesOrErr)
			end

			lastChecked = now
		end

		task.wait(10)
	end
end

local function _retryHandler()
	while true do
		local now = os.time()
		for i = #_pendingMessages, 1, -1 do
			local msg = _pendingMessages[i]
			if msg.nextRetryTime and now >= msg.nextRetryTime then
				local success, err = pcall(function()
					MessagingService:PublishAsync(msg.topic, HttpService:JSONEncode(msg))
				end)
				if success then
					_removePending(msg.uuid)
					_invokeListeners(Monitoring._listeners.onMessageRetried, msg.uuid, msg.topic, msg.retryCount)
				else
					_log("warn", "Retry failed for message", msg.uuid, "error:", err)
					if msg.retryCount >= 5 then
						_removePending(msg.uuid)
						_invokeListeners(Monitoring._listeners.onDeadLetter, msg.uuid, msg.topic, msg.payload, "Max retries reached")
						_log("error", "Message moved to dead letter queue:", msg.uuid, msg.topic)
					else
						_scheduleRetry(msg)
					end
				end
			end
		end
		task.wait(1)
	end
end

--[[
	Replay any messages that were sent after a certain timestamp
	
	@note Messages must have been published with messageRetentionTime > 0 and not expired to be eligible for replay.
]]
function CrossServerManager:ReplayMissedMessages(topic: Topic, sinceTimestamp: number)
	assert(type(topic) == "string", "Topic must be string")
	assert(type(sinceTimestamp) == "number", "sinceTimestamp must be number")

	local map = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME)
	if not map then
		_log("warn", "MemoryStoreService map not available for replay")
		return
	end

	local success, entriesOrErr = pcall(function()
		return map:GetRangeAsync(Enum.SortDirection.Ascending, MAX_RECENT_MESSAGES)
	end)

	if not success or not entriesOrErr then
		_log("warn", "Failed to get entries from MemoryStore for replay:", entriesOrErr)
		return
	end

	for _, entry in ipairs(entriesOrErr) do
		local raw = nil
		local okGet, errGet = pcall(function()
			raw = map:GetAsync(entry.key)
		end)
		if okGet and raw then
			local okDecode, message = pcall(function() return HttpService:JSONDecode(raw) end)
			if okDecode and type(message) == "table" and message.uuid then
				if message.topic == topic and message.timestamp >= sinceTimestamp and not _dedupeCheck(message.uuid) then
					_processMessage(message)
				end
			end
		end
	end
end

--[[
	Flush all pending messages to the MessagingService
	
	@note This function is called on server shutdown
]]
function CrossServerManager:FlushPendingMessages()
	for _, msg in ipairs(_pendingMessages) do
		local success, err = pcall(function()
			MessagingService:PublishAsync(msg.topic, HttpService:JSONEncode(msg))
		end)
		if not success then
			_log("warn", "Failed to flush message on shutdown:", msg.uuid, err)
		end
	end
	_pendingMessages = {}
end

function CrossServerManager:Start()
	if _initialized then _log("warn", "Already initialized") return end
	_initialized = true

	task.delay(2, _checkForUpdate)

	local success, err = pcall(function()
		for topic, _ in pairs(_subscribers) do
			local ok, err = pcall(function()
				MessagingService:SubscribeAsync(topic, _onMessageReceived)
			end)
			if not ok then
				_log("warn", "Failed to subscribe to topic:", topic, err)
			end
		end
	end)

	task.spawn(_memoryStorePoller)
	task.spawn(_retryHandler)

	if RunService:IsServer() then
		local closed = false
		local function onClose()
			if not closed then
				closed = true
				self:FlushPendingMessages()
			end
		end
		game:BindToClose(onClose)
	end

	_log("log", "Initialized")

	task.delay(3, function()
		local Map = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME):GetRangeAsync(Enum.SortDirection.Ascending, 10)

		for _, entry in Map do
			local raw = nil
			local okGet, errGet = pcall(function()
				raw = MemoryStoreService:GetSortedMap(MEMORY_STORE_MAP_NAME):GetAsync(entry.key)
			end)
			if okGet and raw then
				local okDecode, message = pcall(function() return HttpService:JSONDecode(raw) end)
				_fireSubscribers(message.topic, message.uuid, message.payload, 1, message.messageType)
			end
		end
	end)
end

--[[
	Subscribe to monitoring events
]]
function CrossServerManager:MonitoringOn(eventName: string, callback: (any) -> ())
	assert(type(callback) == "function", "Callback must be function")
	local list = Monitoring._listeners[eventName]
	if list then
		table.insert(list, callback)
	else
		error("Unknown monitoring event: "..tostring(eventName))
	end
end

--[[
	Unsubscribe from monitoring events
]]
function CrossServerManager:MonitoringOff(eventName: string, callback: (any) -> ())
	local list = Monitoring._listeners[eventName]
	if list then
		for i = #list, 1, -1 do
			if list[i] == callback then
				table.remove(list, i)
			end
		end
	end
end

--[[
	Get the server ID
	
	@return string
]]
function CrossServerManager:GetServerId()
	return _serverId
end

return CrossServerManager
