--
-- Copyright (C) 2016 Meng Zhang @ Yottaa,Inc
--
local consts = require("amqp.consts")
local frame = require("amqp.frame")
local log = require("log")
local json = require("json")

local bit = require("bit")
local band = bit.band
local bor = bit.bor
local lshift = bit.lshift
local rshift = bit.rshift

local format = string.format
local gmatch = string.gmatch
local min = math.min

local socket = require("socket")
local tcp = socket.tcp

local amqp = {}
local mt = { __index = amqp }

-- to check whether we have valid parameters to setup
local function mandatory_options(opts)
    if not opts then
        error("no opts provided.")
    end

    if type(opts) ~= "table" then
        error("opts is not valid.")
    end

    if (opts.role == nil or opts.role == "consumer") and not opts.queue then
        error("as a consumer, queue is required.")
    end

    if (opts.role == "producer") and not opts.exchange then
        error("no exchange configured.")
    end
end

--
-- initialize the context
--
function amqp.new(opts)
    mandatory_options(opts)

    local sock, err = tcp()
    if not sock then
        return nil, err
    end

    return setmetatable({
        sock = sock,
        opts = opts,
        connection_state = consts.state.CLOSED,
        channel_state = consts.state.CLOSED,

        major = consts.PROTOCOL_VERSION_MAJOR,
        minor = consts.PROTOCOL_VERSION_MINOR,
        revision = consts.PROTOCOL_VERSION_REVISION,

        frame_max = consts.DEFAULT_FRAME_SIZE,
        channel_max = consts.DEFAULT_MAX_CHANNELS,
        mechanism = consts.MECHANISM_PLAIN
    }, mt)
end

local function sslhandshake(ctx)
    local sock = ctx.sock

    local ssl = require("ssl")
    local params = {
        mode = "client",
        protocol = "sslv23",
        verify = "none",
        options = {"all", "no_sslv2", "no_sslv3"}
    }

    ctx.sock = ssl.wrap(sock, params)
    local ok, msg = ctx.sock:dohandshake()
    if not ok then
        log.error("[amqp.connect] SSL handshake failed: %s", tostring(msg))
    else
        log.error("[amqp.connect] SSL handshake.")
    end
    return ok, msg
end
--
-- connect to the broker
--
function amqp:connect(...)
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    -- configurable but 5 seconds timeout
    sock:settimeout(self.opts.connect_timeout or 5000)

    local ok, err = sock:connect(...)
    if not ok then
        log.error("[amqp.connect] failed: %s", tostring(err))
        return nil, err
    end

    if self.opts.ssl then
        return sslhandshake(self)
    end
    return true
end

--
-- to close the socket
--
function amqp:close()
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    return sock:close()
end


local function platform()
    if jit and jit.os and jit.arch then
        return jit.os .. "_" .. jit.arch
    end
    return "posix"
end

--
-- connection and channel
--

local function connection_start_ok(ctx)

    local user = ctx.opts.user or "guest"
    local password = ctx.opts.password or "guest"
    local f = frame.new_method_frame(consts.DEFAULT_CHANNEL,
                                    consts.class.CONNECTION,
                                    consts.method.connection.START_OK)
    f.method = {
        properties = {
            product = consts.PRODUCT,
            version = consts.VERSION,
            platform = platform(),
            copyright = consts.COPYRIGHT,
            capabilities = {
                authentication_failure_close = true
            }
        },
        mechanism = ctx.mechanism,
        response = format("\0%s\0%s", user, password),
        locale = consts.LOCALE
    }

    return frame.wire_method_frame(ctx, f)
end

local function connection_tune_ok(ctx)

    local f = frame.new_method_frame(consts.DEFAULT_CHANNEL,
                                    consts.class.CONNECTION,
                                    consts.method.connection.TUNE_OK)

    f.method = {
        channel_max = ctx.channel_max or consts.DEFAULT_MAX_CHANNELS,
        frame_max = ctx.frame_max or consts.DEFAULT_FRAME_SIZE,
        heartbeat = ctx.opts.heartbeat or consts.DEFAULT_HEARTBEAT
    }

    local msg = f:encode()
    local sock = ctx.sock
    local bytes, err = sock:send(msg)
    if not bytes then
        return nil, "[connection_tune_ok]" .. err
    end
    log.debug("[connection_tune_ok] wired a frame.", "[class_id]: ", f.class_id, "[method_id]: ", f.method_id)
    return true
end

local function connection_open(ctx)
    local f = frame.new_method_frame(consts.DEFAULT_CHANNEL,
                                    consts.class.CONNECTION,
                                    consts.method.connection.OPEN)
    f.method = {
        virtual_host = ctx.opts.virtual_host or "/"
    }

    return frame.wire_method_frame(ctx, f)
end


local function sanitize_close_reason(ctx, reason)
    reason = reason or {}
    local ongoing = ctx.ongoing or {}
    return {
        reply_code = reason.reply_code or consts.err.CONNECTION_FORCED,
        reply_text = reason.reply_text or "",
        class_id = ongoing.class_id or 0,
        method_id = ongoing.method_id or 0
    }
end


local function connection_close(ctx, reason)
    local f = frame.new_method_frame(consts.DEFAULT_CHANNEL,
                                    consts.class.CONNECTION,
                                    consts.method.connection.CLOSE)

    f.method = sanitize_close_reason(ctx, reason)
    return frame.wire_method_frame(ctx, f)
end


local function connection_close_ok(ctx)
    local f = frame.new_method_frame(ctx.channel or 1,
                                    consts.class.CONNECTION,
                                    consts.method.connection.CLOSE_OK)

    return frame.wire_method_frame(ctx, f)
end

local function channel_open(ctx)
    local f = frame.new_method_frame(ctx.opts.channel or 1,
                                    consts.class.CHANNEL,
                                    consts.method.channel.OPEN)
    local msg = f:encode()
    local sock = ctx.sock
    local bytes, err = sock:send(msg)
    if not bytes then
        return nil, "[channel_open]" .. err
    end

    log.debug("[channel_open] wired a frame. [class_id]: %s [method_id]: %s",
                                    tostring(f.class_id), tostring(f.method_id))
    local res = frame.consume_frame(ctx)
    if res then
        log.debug("[channel_open] channel: %s", tostring(res.channel))
    end
    return res
end

local function is_version_acceptable(ctx, major, minor)
    return ctx.major == major and ctx.minor == minor
end

local function is_mechanism_acceptable(ctx, method)
    local mechanism = method.mechanism
    if not mechanism then
        return nil, "broker does not support any mechanism."
    end

    for me in gmatch(mechanism, "%S+") do
        if me == ctx.mechanism then
            return true
        end
    end

    return nil, "mechanism does not match"
end

local function verify_capablities(ctx, method)

    if not is_version_acceptable(ctx, method.major,method.minor) then
        return nil, "protocol version does not match."
    end

    if not is_mechanism_acceptable(ctx,method) then
        return nil, "mechanism does not match."
    end
    return true
end


local function negotiate_connection_tune_params(ctx, method)
    if not method then
        return
    end

    if method.channel_max ~= nil and method.channel_max ~= 0 then
        -- 0 means no limit
        ctx.channel_max = min(ctx.channel_max, method.channel_max)
    end

    if method.frame_max ~= nil and method.frame_max ~= 0 then
        ctx.frame_max = min(ctx.frame_max, method.frame_max)
    end
end

local function set_state(ctx, channel_state, connection_state)
    ctx.channel_state = channel_state
    ctx.connection_state = connection_state
end

function amqp:setup()
    local sock = self.sock
    if not sock then
        return nil, "not initialized"
    end

    -- configurable but 30 seconds read timeout
    sock:settimeout(self.opts.read_timeout or 30000)

    local res, err = frame.wire_protocol_header(self)
    if not res then
        log.error("[amqp.setup] wire_protocol_header failed: %s" .. tostring(err))
        return nil, err
    end

    if res.method then
        log.debug("[amqp.setup] connection_start: %s", json.encode(res.method))
        local ok
        ok, err = verify_capablities(self, res.method)
        if not ok then
            -- in order to close the socket without sending futher data
            set_state(self, consts.state.CLOSED, consts.state.CLOSED)
            return nil, err
        end
    end

    res, err = connection_start_ok(self)
    if not res then
        log.error("[amqp.setup] connection_start_ok failed: %s", tostring(err))
        return nil, err
    end

    negotiate_connection_tune_params(self, res.method)

    res, err = connection_tune_ok(self)
    if not res then
        log.error("[amqp.setup] connection_tune_ok failed: %s", tostring(err))
        return nil, err
    end

    res, err = connection_open(self)
    if not res then
        log.error("[amqp.setup] connection_open failed: %s", tostring(err))
        return nil, err
    end

    self.connection_state = consts.state.ESTABLISHED

    res, err = channel_open(self)
    if not res then
        log.error("[amqp.setup] channel_open failed: %s", tostring(err))
        return nil, err
    end
    
    -- added by Dmitry
    res, err = self:basic_qos()
    if not res then
        log.error("[amqp.setup] basic_qos failed: %s", tostring(err))
        return nil, err
    end
    -- ------------------------------
    
    self.channel_state = consts.state.ESTABLISHED
    return true
end

--
-- close channel and connection if needed.
--
function amqp:teardown(reason)
    -- When a connection is closed, so are all channels on it.
    -- https://www.rabbitmq.com/channels.html
    if self.connection_state == consts.state.ESTABLISHED then
        local ok, err = connection_close(self, reason)
        if not ok then
            log.error("[connection_close] err: %s", tostring(err))
            return ok, err
        end
    elseif self.connection_state == consts.state.CLOSE_WAIT then
        local ok, err = connection_close_ok(self)
        if not ok then
            log.error("[connection_close_ok] err: %s", tostring(err))
            return ok, err
        end
    end

    return true
end

--
-- initialize the consumer
--
local function prepare_to_consume(ctx)

    if ctx.channel_state ~= consts.state.ESTABLISHED then
        return nil, "[prepare_to_consume] channel is not open."
    end

    local res, err = amqp.queue_declare(ctx)
    if not res then
        log.error("[prepare_to_consume] queue_declare failed: %s", tostring(err))
        return nil, err
    end

    if not ctx.opts.no_bind then
      res, err = amqp.queue_bind(ctx)
      if not res then
          log.error("[prepare_to_consume] queue_bind failed: %s", tostring(err))
          return nil, err
      end
    end

    res, err = amqp.basic_consume(ctx)
    if not res then
        log.error("[prepare_to_consume] basic_consume failed: %s", tostring(err))
        return nil, err
    end

    return true
end

--
-- conclude a heartbeat timeout
-- if and only if we see ctx.threshold or more failure heartbeats in the recent heartbeats ctx.window
--

local function timedout(ctx, timeouts)
    local window = ctx.window or 5
    local threshold = ctx.threshold or 4
    local c = 0
    for i = 1, window do
        if band(rshift(timeouts, i - 1), 1) ~= 0 then
            c = c + 1
        end
    end
    return c >= threshold
end

local function error_string(err)
    if err then
        return err
    end
    return "?"
end

local function exiting()
    return nil
end

--
-- consumer
--
function amqp:consume()

    local ok, err = self:setup()
    if not ok then
        self:teardown()
        return nil, err
    end

    ok, err = prepare_to_consume(self)
    if not ok then
        self:teardown()
        return nil, err
    end

    local hb = {
        last = os.time(),
        timeouts = 0
    }

    while true do
--
        ::continue::
--
        local f, err0 = frame.consume_frame(self)
        if not f then

        if exiting() then
            err = "exiting"
            break
        end
        -- in order to send the heartbeat,
        -- the very read op need be awaken up periodically, so the timeout is expected.
        if err0 ~= "timeout" then
            log.error("[amqp.consume] %s", tostring(error_string(err0)))
        end

        if err0 == "closed" then
            err = err0
            set_state(self, consts.state.CLOSED, consts.state.CLOSED)
            log.error("[amqp.consume] socket closed.")
            break
        end

        if err0 == "wantread" then
            err = err0
            set_state(self, consts.state.CLOSED, consts.state.CLOSED)
            log.error("[amqp.consume] SSL socket needs to dohandshake again.")
            break
        end

        -- intented timeout?
        local now = os.time()
        if now - hb.last > consts.DEFAULT_HEARTBEAT then
            log.info("[amqp.consume] timeouts inc. [ts]: %s", tostring(now))
            hb.timeouts = bor(lshift(hb.timeouts, 1), 1)
            hb.last = now
            ok, err0 = frame.wire_heartbeat(self)
            if not ok then
                log.error("[heartbeat] pong error: %s [ts]: %s", tostring(error_string(err0)), tostring(hb.last))
            else
                log.debug("[heartbeat] pong sent. [ts]: %s", tostring(hb.last))
            end
        end

        if timedout(self,hb.timeouts) then
            err = "heartbeat timeout"
            log.error("[amqp.consume] timedout. [ts]: %s", tostring(now))
            break
        end

        log.debug("[amqp.consume] continue consuming %s", tostring(err0))
            goto continue
        end

        if f.type == consts.frame.METHOD_FRAME then
            if f.class_id == consts.class.CHANNEL then
                if f.method_id == consts.method.channel.CLOSE then
                    set_state(self, consts.state.CLOSE_WAIT, self.connection_state)
                    log.info("[channel close method] %s %s", tostring(f.method.reply_code),
                                                                tostring(f.method.reply_text))
                    break
                end
            elseif f.class_id == consts.class.CONNECTION then
                if f.method_id == consts.method.connection.CLOSE then
                    set_state(self, consts.state.CLOSED, consts.state.CLOSE_WAIT)
                    log.info("[connection close method] %s %s", tostring(f.method.reply_code),
                                                                tostring(f.method.reply_text))
                    break
                end
            elseif f.class_id == consts.class.BASIC then
                if f.method_id == consts.method.basic.DELIVER then
                    if f.method ~= nil then
                        log.debug("[basic_deliver] %s", tostring(f.method))
                    end
                end
            end

        elseif f.type == consts.frame.HEADER_FRAME then
                log.debug("[header] class_id: %d weight: %d, body_size: %d",
                                                    tostring(f.class_id), tostring(f.weight), tostring(f.body_size))
                log.debug("[frame.properties] %s", tostring(f.properties))

        elseif f.type == consts.frame.BODY_FRAME then
            if self.opts.callback then
                local status
                status, err0 = pcall(self.opts.callback, f.body)
                if not status then
                    log.error("calling callback failed: %s", tostring(err0))
                end
            end
            log.debug("[body] %s", tostring(f.body))

        elseif f.type == consts.frame.HEARTBEAT_FRAME then
            hb.last = os.time()
            log.info("[heartbeat] ping received. [ts]: %s", tostring(hb.last))
            hb.timeouts = band(lshift(hb.timeouts, 1), 0)
            ok, err0 = frame.wire_heartbeat(self)
            if not ok then
                log.error("[heartbeat] pong error: %s [ts]: ", tostring(error_string(err0)), tostring(hb.last))
            else
                log.debug("[heartbeat] pong sent. [ts]: %s", tostring(hb.last))
            end
        end
    end

    self:teardown()
    return nil, err
end

--
-- publisher
--

function amqp:publish(payload)

    local size = #payload
    local ok, err = amqp.basic_publish(self)
    if not ok then
        log.error("[amqp.publish] failed: %s", tostring(err))
        return nil, err
    end

    ok, err = frame.wire_header_frame(self, size)
    if not ok then
        log.error("[amqp.publish] failed: %s", tostring(err))
        return nil, err
    end

    local max = self.frame_max

    local pos = 0
    local left = size

    while left > 0 do
        local chunk
        if left < max then
            chunk = left
        else
            chunk = max
        end

        ok, err = frame.wire_body_frame(self, payload, pos + 1, pos + chunk)
        if not ok then
            log.error("[amqp.publish] failed: %s", tostring(err))
            return nil, err
        end

        pos = pos + chunk
        left = left - chunk

    end

    return true
end

--
-- queue
--
function amqp:queue_declare(opts)

    opts = opts or {}

    if not opts.queue and not self.opts.queue then
        return nil, "[queue_declare] queue is not specified."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.QUEUE,
                                        consts.method.queue.DECLARE)

    f.method = {
        queue = opts.queue or self.opts.queue,
        passive = opts.passive or self.opts.passive or false,
        durable = opts.durable or self.opts.durable or false,
        exclusive = opts.exclusive or self.opts.exclusive or false,
        auto_delete = opts.auto_delete or self.opts.auto_delete or false,
        no_wait = self.opts.no_wait or true
    }
    return frame.wire_method_frame(self, f)
end

function amqp:queue_bind(opts)

    opts = opts or {}

    if not opts.queue and not self.opts.queue then
        return nil, "[queue_bind] queue is not specified."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.QUEUE,
                                        consts.method.queue.BIND)

    f.method = {
        queue = opts.queue or self.opts.queue,
        exchange = opts.exchange or self.opts.exchange,
        routing_key = opts.routing_key or self.opts.routing_key,
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end

function amqp:queue_unbind(opts)

    opts = opts or {}

    if not opts.queue and not self.opts.queue then
        return nil, "[queue_unbind] queue is not specified."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.QUEUE,
                                        consts.method.queue.UNBIND)

    f.method = {
        queue = opts.queue or self.opts.queue,
        exchange = opts.exchange or self.opts.exchange,
        routing_key = opts.routing_key or "",
    }

    return frame.wire_method_frame(self, f)
end

function amqp:queue_delete(opts)

    opts = opts or {}

    if not opts.queue and not self.opts.queue then
        return nil, "[queue_delete] queue is not specified."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.QUEUE,
                                        consts.method.queue.DELETE)

    f.method = {
        queue = opts.queue or self.opts.queue,
        if_unused = opts.if_unused or false,
        if_empty = opts.if_empty or false,
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end

--
-- exchange
--
function amqp:exchange_declare(opts)

    opts = opts or {}

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.EXCHANGE,
                                        consts.method.exchange.DECLARE)

    f.method = {
        exchange = opts.exchange or self.opts.exchange,
        typ = opts.typ or "topic",
        passive = opts.passive or false,
        durable = opts.durable or self.opts.durable or false,
        auto_delete = opts.auto_delete or self.opts.auto_delete or false,
        internal = opts.internal or self.opts.internal or false,
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end

function amqp:exchange_bind(opts)

    if not opts then
        return nil, "[exchange_bind] opts is required."
    end

    if not opts.source then
        return nil, "[exchange_bind] source is required."
    end

    if not opts.destination then
        return nil, "[exchange_bind] destination is required."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.EXCHANGE,
                                        consts.method.exchange.BIND)

    f.method = {
        destination = opts.destination,
        source = opts.source,
        routing_key = opts.routing_key or "",
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end

function amqp:exchange_unbind(opts)

    if not opts then
        return nil, "[exchange_unbind] opts is required."
    end

    if not opts.source then
        return nil, "[exchange_unbind] source is required."
    end

    if not opts.destination then
        return nil, "[exchange_unbind] destination is required."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.EXCHANGE,
                                        consts.method.exchange.UNBIND)

    f.method = {
        destination = opts.destination,
        source = opts.source,
        routing_key = opts.routing_key or "",
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end

function amqp:exchange_delete(opts)

    opts = opts or {}

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.EXCHANGE,
                                        consts.method.exchange.DELETE)

    f.method = {
        exchange = opts.exchange or self.opts.exchange,
        if_unused = opts.is_unused or true,
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end

--
-- basic
--
function amqp:basic_consume(opts)

    opts = opts or {}

    if not opts.queue and not self.opts.queue then
        return nil, "[basic_consume] queue is not specified."
    end

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.BASIC,
                                        consts.method.basic.CONSUME)

    f.method = {
        queue = opts.queue or self.opts.queue,
        consumer_tag = opts.consumer_tag or self.opts.consumer_tag,  -- added by Dmitry
        no_local = opts.no_local or false,
        no_ack = opts.no_ack or true,
        exclusive = opts.exclusive or false,
        no_wait = self.opts.no_wait or false
    }

    return frame.wire_method_frame(self, f)
end


function amqp:basic_publish(opts)

    opts = opts or {}

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.BASIC,
                                        consts.method.basic.PUBLISH)
    f.method = {
        exchange = opts.exchange or self.opts.exchange,
        routing_key = opts.routing_key or self.opts.routing_key or "",
        mandatory = opts.mandatory or false,
        immediate = opts.immediate or false
    }

    local msg = f:encode()
    local sock = self.sock
    local bytes, err = sock:send(msg)
    if not bytes then
        return nil,"[basic_publish]" .. err
    end
    return bytes
end

-- added by dmitry
function amqp:basic_qos(opts)
    opts = opts or {}

    local f = frame.new_method_frame(self.channel or 1,
                                        consts.class.BASIC,
                                        consts.method.basic.QOS)

    f.method = {
        prefetch_count = opts.prefetch_count or self.opts.prefetch_count or 0,
        global = opts.prefetch_global or self.opts.prefetch_global,
        prefetch_size = opts.prefetch_size or self.opts.prefetch_size or 0
    }
    return frame.wire_method_frame(self, f)
end

return amqp

