local amqp = require('amqp')
local json = require('json')


local callback = function(body, properties)
  print(body)
  if properties then
    print(json.encode(properties or {}))
  end
  if body == 'quit' then return end
end

local opts = {
  user = 'guest',
  password = 'guest',
  virtual_host = 'workhost',
  queue='work_q',
  passive = true,
  no_bind = true,
  callback=callback,
}
local ctx = amqp.new(opts)
ctx:connect('localhost', '5672')

return {
  ctx = ctx,
  opts = opts,
  callback = callback
}
