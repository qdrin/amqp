local amqp = require('amqp')
local fiber = require('fiber')
local json = require('json')

local function consume_callback_wrapper(ch)
    return function(payload)
        ch:put(payload)
    end
end

local function get_worker(args)
  local wrk = {
    channel_out = fiber.channel(1),
  }
  local w_ok, w_err
  wrk.ctx = amqp.new({
      role = "consumer",
      queue = args.queue,
      virtual_host = args.vhost,
      exchange = args.exchange,
      routing_key = args.routing_key,
      ssl = false,
      passive = args.passive,
      durable = args.durable,
      auto_delete = args.auto_delete,
      user = args.user,
      password = args.password,
      prefetch_count = args.prefetch_count,
      consumer_tag = args.consumer_tag,
      no_bind = args.no_bind,
      callback = consume_callback_wrapper(wrk.channel_out),
  })
  w_ok, w_err = wrk.ctx:connect('127.0.0.1', 5672)
  if not w_ok then return nil, w_err end

  wrk.consume = function(self)
    self.consume_fiber = fiber.create(self.ctx.consume, self.ctx)
  end

  wrk.close = function(self)
    local ok, err = self.ctx:close()
    if self.consume_fiber:status() ~= 'dead' then self.consume_fiber:cancel() end
    return ok, err
  end
  return wrk
end

local function get_rmq_data()
  local hc = require('http.client')
  local auth = 'Authorization: Basic Z3Vlc3Q6Z3Vlc3Q='
  local resp = hc.get('http://localhost:15672/api/queues/workhost/work_q',
    {headers = {Authorization='Basic Z3Vlc3Q6Z3Vlc3Q='}}
  )
  if resp.status ~= 200 then return nil, resp.status end
  return json.decode(resp.body)
end

return {
    get_worker = get_worker,
    get_rmq_data = get_rmq_data,
}