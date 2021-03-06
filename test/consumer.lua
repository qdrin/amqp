local amqp = require('amqp')
local tap = require('tap')
local fiber = require('fiber')
local json = require('json')
local helper = require('test.helpers.helper')

package.setsearchroot()

local wait_time = 5

local test_basic = function(test)
    test:plan(15)

    test:diag("Client should be able to consume messages")
    local get_worker = helper.get_worker

    local ctx = amqp.new({
        role = "producer",
        exchange = "work.pub",
        routing_key = "work.rk",
        virtual_host = "workhost",
        passive = true,
        auto_delete = false,
        durable = true,
        ssl = false,
        user = "guest",
        password = "guest",
    })
    local c_args = {
        role = "consumer",
        queue = "work_q",
        vhost = "workhost",
        exchange = "work.pub",
        routing_key = "work.rk",
        durable = true,
        passive = true,
        ssl = false,
        user = "read_user",
        password = "read_user",
    }
    test:isnt(ctx, nil, 'Ctx created')

    local ok, err = ctx:connect("127.0.0.1", 5672)
    test:ok(ok, "Connect status ok")
    test:is(err, nil, "No error on connect")

    ok, err = ctx:setup()
    test:ok(ok, "Setup status ok")
    test:is(err, nil, "No error on setup")

    ok, err = ctx:publish("Hello world!")
    test:ok(ok, "Publish status ok")
    test:is(err, nil, "No error on publish")

    local wrk
    wrk, err = get_worker(c_args)
    test:ok(ok, "Consume worker creation ok")
    wrk:consume()
    local res = wrk.channel_out:get(0.2)
    ok = (res == "Hello world!")
    test:ok(ok, "Consume status ok")
    ok, err = wrk:close()
    test:ok(ok, "Close consumer status ok")
    test:is(err, nil, "No error on consumer close")

    ok, err = ctx:teardown()
    test:ok(ok, "Teardown status ok")
    test:is(err, nil, "No error on teardown")

    ok, err = ctx:close()
    test:ok(ok, "Close status ok")
    test:is(err, nil, "No error on close")
end

local test_no_routing_key = function(test)
    test:plan(1)

    test:diag("Client should not be able to consume messages with exchange and without routing_key")

    local c_args = {
      role = "consumer",
      queue = "work_q",
      vhost = "workhost",
      exchange = "work.pub",
      durable = true,
      passive = true,
      ssl = false,
      user = "read_user",
      password = "read_user",
    }
    local is_ok, ctx = pcall(amqp.new, c_args)
    test:is(is_ok, false, 'Ctx creation rejected ok')
end

local test_no_binding = function(test)
  test:plan(15)

  test:diag("Client should be able to consume messages without creating binds")
  local get_worker = helper.get_worker

  local ctx = amqp.new({
      role = "producer",
      exchange = "work.pub",
      routing_key = "work.rk",
      virtual_host = "workhost",
      passive = true,
      auto_delete = false,
      durable = true,
      ssl = false,
      user = "guest",
      password = "guest",
  })
  local c_args = {
      role = "consumer",
      queue = "work_q",
      vhost = "workhost",
      durable = true,
      passive = true,
      ssl = false,
      user = "read_user",
      password = "read_user",
      no_bind = true,
  }
  test:isnt(ctx, nil, 'Ctx created')

  local ok, err = ctx:connect("127.0.0.1", 5672)
  test:ok(ok, "Connect status ok")
  test:is(err, nil, "No error on connect")

  ok, err = ctx:setup()
  test:ok(ok, "Setup status ok")
  test:is(err, nil, "No error on setup")

  ok, err = ctx:publish("Hello world!")
  test:ok(ok, "Publish status ok")
  test:is(err, nil, "No error on publish")

  local wrk
  wrk, err = get_worker(c_args)
  test:ok(ok, "Consume worker creation ok")
  wrk:consume()
  local res = wrk.channel_out:get(0.2)
  ok = (res == "Hello world!")
  test:ok(ok, "Consume status ok")
  ok, err = wrk:close()
  test:ok(ok, "Close consumer status ok")
  test:is(err, nil, "No error on consumer close")

  ok, err = ctx:teardown()
  test:ok(ok, "Teardown status ok")
  test:is(err, nil, "No error on teardown")

  ok, err = ctx:close()
  test:ok(ok, "Close status ok")
  test:is(err, nil, "No error on close")
end

local test_prefetch_count = function(test)
  test:plan(15)

  test:diag("Client should use prefetch_count for safe connection")
  local get_worker = helper.get_worker

  local ctx = amqp.new({
      role = "producer",
      exchange = "work.pub",
      routing_key = "work.rk",
      virtual_host = "workhost",
      passive = true,
      auto_delete = false,
      durable = true,
      ssl = false,
      user = "guest",
      password = "guest",
  })
  local c_args = {
      role = "consumer",
      queue = "work_q",
      vhost = "workhost",
      durable = true,
      passive = true,
      ssl = false,
      user = "read_user",
      password = "read_user",
      prefetch_count = 10,
      no_bind = true,
  }
  test:isnt(ctx, nil, 'Ctx created')

  local ok, err = ctx:connect("127.0.0.1", 5672)
  test:ok(ok, "Connect status ok")
  test:is(err, nil, "No error on connect")

  ok, err = ctx:setup()
  test:ok(ok, "Setup status ok")
  test:is(err, nil, "No error on setup")

  local wrk
  wrk, err = get_worker(c_args)
  test:ok(ok, "Consume worker creation ok")
  wrk:consume()
  fiber.sleep(wait_time)
  local res
  res, err = helper.get_rmq_data()
  test:ok(res, 'rmq data get ok')
  test:is(#res.consumer_details, 1, 'consumer_details contains 1 element')
  test:is(res.consumer_details[1].prefetch_count, 10, 'prefetch_count ok')
  ok, err = wrk:close()
  test:ok(ok, "Close consumer status ok")
  test:is(err, nil, "No error on consumer close")

  ok, err = ctx:teardown()
  test:ok(ok, "Teardown status ok")
  test:is(err, nil, "No error on teardown")

  ok, err = ctx:close()
  test:ok(ok, "Close status ok")
  test:is(err, nil, "No error on close")
end

local test_consumer_tag = function(test)
  test:plan(15)

  test:diag("Client should use consumer-tag for connection")
  local get_worker = helper.get_worker

  local ctx = amqp.new({
      role = "producer",
      exchange = "work.pub",
      routing_key = "work.rk",
      virtual_host = "workhost",
      passive = true,
      auto_delete = false,
      durable = true,
      ssl = false,
      user = "guest",
      password = "guest",
  })
  local c_args = {
      role = "consumer",
      queue = "work_q",
      vhost = "workhost",
      exchange = "work.pub",
      routing_key = "work.rk",
      durable = true,
      passive = true,
      ssl = false,
      consumer_tag = "test_consumer_tag",
      user = "read_user",
      password = "read_user",
      prefetch_count = 10,
  }
  test:isnt(ctx, nil, 'Ctx created')

  local ok, err = ctx:connect("127.0.0.1", 5672)
  test:ok(ok, "Connect status ok")
  test:is(err, nil, "No error on connect")

  ok, err = ctx:setup()
  test:ok(ok, "Setup status ok")
  test:is(err, nil, "No error on setup")

  local wrk
  wrk, err = get_worker(c_args)
  test:ok(ok, "Consume worker creation ok")
  wrk:consume()
  fiber.sleep(wait_time)
  local res
  res, err = helper.get_rmq_data()
  test:ok(res, 'rmq data get ok')
  test:is(#res.consumer_details, 1, 'consumer_details contains 1 element')
  test:diag(res.consumer_details[1].consumer_tag)
  test:ok((res.consumer_details[1].consumer_tag == "test_consumer_tag"), 'consumer_tag ok')
  ok, err = wrk:close()
  test:ok(ok, "Close consumer status ok")
  test:is(err, nil, "No error on consumer close")

  ok, err = ctx:teardown()
  test:ok(ok, "Teardown status ok")
  test:is(err, nil, "No error on teardown")

  ok, err = ctx:close()
  test:ok(ok, "Close status ok")
  test:is(err, nil, "No error on close")
end

local test_consumer_properties = function(test)
    test:plan(19)
  
    test:diag("Client should use consumer properties for connection")
    local get_worker = helper.get_worker
    local properties = {
        product = "test_product_app",
        os = io.popen("uname -oi"):read(),
        copyright = "(c) Nexign, JSC, 2021-2022",
    }
    local ctx = amqp.new({
        role = "producer",
        exchange = "work.pub",
        routing_key = "work.rk",
        virtual_host = "workhost",
        passive = true,
        auto_delete = false,
        durable = true,
        ssl = false,
        user = "guest",
        password = "guest",
        properties = properties,
    })
    local c_args = {
        role = "consumer",
        queue = "work_q",
        vhost = "workhost",
        exchange = "work.pub",
        routing_key = "work.rk",
        durable = true,
        passive = true,
        ssl = false,
        consumer_tag = "test_consumer_tag",
        user = "read_user",
        password = "read_user",
        prefetch_count = 10,
    }
    test:isnt(ctx, nil, 'Ctx created')
  
    local ok, err = ctx:connect("127.0.0.1", 5672)
    test:ok(ok, "Connect status ok")
    test:is(err, nil, "No error on connect")
  
    ok, err = ctx:setup()
    test:ok(ok, "Setup status ok")
    test:is(err, nil, "No error on setup")
  
    local wrk
    wrk, err = get_worker(c_args)
    test:ok(ok, "Consume worker creation ok")
    wrk:consume()
    fiber.sleep(wait_time)
    local res
    res, err = helper.get_connection_data()
    test:ok(res, 'rmq connection data get ok')
    test:ok(res[1].client_properties, 1, 'client_properties exists')
    local client_props = res[1].client_properties
    test:is(client_props.product, properties.product, 'client_properties.product is correct')
    test:is(client_props.os, properties.os, 'client_properties.os is correct')
    test:is(client_props.copyright, properties.copyright, 'client_properties.copyright is correct')
    test:is(client_props.platform, io.popen("uname -oi"):read(), 'client_properties.platform is default')
    test:is(client_props.host, io.popen("hostname -f"):read(), 'client_properties.host is default')

    -- The commented part waits for "quit" message in 'work_q queue
    -- and may be useful when you need to manually check customer connection on localhost:15672
    -- ok = false
    -- while not ok do
    --     local res = wrk.channel_out:get(0.2)
    --     ok = (res == "quit")
    --     test:diag(tostring(res))
    --     test:diag(tostring(ok))
    --     fiber.sleep(wait_time)
    -- end
    ok, err = wrk:close()
    test:ok(ok, "Close consumer status ok")
    test:is(err, nil, "No error on consumer close")
  
    ok, err = ctx:teardown()
    test:ok(ok, "Teardown status ok")
    test:is(err, nil, "No error on teardown")
  
    ok, err = ctx:close()
    test:ok(ok, "Close status ok")
    test:is(err, nil, "No error on close")
  end

local test = tap.test('amqp')
test:plan(6)
test:test('basic', test_basic)
test:test('no_routing_key', test_no_routing_key)
test:test('test_no_binding', test_no_binding)
test:test('prefetch_count', test_prefetch_count)
test:test('consumer_tag', test_consumer_tag)
test:test('consumer_properties', test_consumer_properties)

os.exit(test:check() == true and 0 or -1)
