local amqp = require('amqp')
local tap = require('tap')
local json = require('json')

local test_basic = function(test)
    test:plan(11)

    test:diag("Client should be able to publish messages")

    local ctx = amqp.new({
        role = "producer",
        exchange = "work.pub",
        routing_key = "work.rk",
        ssl = false,
        user = "guest",
        password = "guest",
        virtual_host = "workhost",
    })
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

    ok, err = ctx:teardown()
    test:ok(ok, "Teardown status ok")
    test:is(err, nil, "No error on teardown")

    ok, err = ctx:close()
    test:ok(ok, "Close status ok")
    test:is(err, nil, "No error on close")
end

local test_basic_with_headers = function(test)
  test:plan(11)

  test:diag("Client should be able to publish messages with custom headers")

  local ctx = amqp.new({
      role = "producer",
      exchange = "work.pub",
      routing_key = "work.rk",
      ssl = false,
      user = "guest",
      password = "guest",
      virtual_host = "workhost",
  })
  test:isnt(ctx, nil, 'Ctx created')

  local ok, err = ctx:connect("127.0.0.1", 5672)
  test:ok(ok, "Connect status ok")
  test:is(err, nil, "No error on connect")

  ok, err = ctx:setup()
  test:ok(ok, "Setup status ok")
  test:is(err, nil, "No error on setup")

  ok, err = ctx:publish("Hello world!", {headers = {['header-name']="header-value"}})
  test:ok(ok, "Publish status ok")
  test:is(err, nil, "No error on publish")

  ok, err = ctx:teardown()
  test:ok(ok, "Teardown status ok")
  test:is(err, nil, "No error on teardown")

  ok, err = ctx:close()
  test:ok(ok, "Close status ok")
  test:is(err, nil, "No error on close")
end

local test_connection = function(test)
    test:plan(9)

    test:diag("Client should be rejected with wrong user name or password")

    local ctx = amqp.new({
        role = "consumer",
        queue = "work_q",
        exchange = "work.pub",
        routing_key = "work.rk",
        virtual_host = "workhost",
        ssl = false,
        user = "guest",
        password = "guest0"
    })
    test:isnt(ctx, nil, 'Ctx created')

    local ok, err = ctx:connect("127.0.0.1", 5672)
    test:ok(ok, "Connect status ok")
    test:is(err, nil, "No error on connect")

    ok, err = ctx:setup()
    test:is(ok, nil, "Setup status error")
    test:is(err, 403, "Access denied")

    ok, err = ctx:teardown()
    test:ok(ok, "Teardown status ok")
    test:is(err, nil, "No error on teardown")

    ok, err = ctx:close()
    test:ok(ok, "Close status ok")
    test:is(err, nil, "No error on close")
end

local test_exchange = function(t)
    local test_declare = function(test, options)
        test:plan(11)

        test:diag("Client should be able to declare exchange")
        local ctx = amqp.new(options)
        test:isnt(ctx, nil, 'Ctx created')

        local ok, err = ctx:connect("127.0.0.1", 5672)
        test:ok(ok, "Connect status ok")
        test:is(err, nil, "No error on connect")

        ok, err = ctx:setup()
        test:ok(ok, "Setup status ok")
        test:is(err, nil, "No error on setup")

        ok, err = amqp.exchange_declare(ctx, {
            exchange = "work.pub",
            passive = false,
            durable = true,
            internal = false,
            auto_delete = false
        })
        test:ok(ok, "Exchange declare status ok")
        test:is(err, nil, "No error on exchange declare")

        ok, err = ctx:teardown()
        test:ok(ok, "Teardown status ok")
        test:is(err, nil, "No error on teardown")

        ok, err = ctx:close()
        test:ok(ok, "Close status ok")
        test:is(err, nil, "No error on close")
    end

    local test_bind = function(test, options)
        test:plan(11)

        test:diag("Client should be able to bind exchange")
        local ctx = amqp.new(options)
        test:isnt(ctx, nil, 'Ctx created')

        local ok, err = ctx:connect("127.0.0.1", 5672)
        test:ok(ok, "Connect status ok")
        test:is(err, nil, "No error on connect")

        ok, err = ctx:setup()
        test:ok(ok, "Setup status ok")
        test:is(err, nil, "No error on setup")

        ok, err = amqp.exchange_bind(ctx, {
            source = "work.pub",
            destination = "work_dup.pub",
            routing_key = "work.rk.new"
        })
        test:ok(ok, "Exchange bind status ok")
        test:is(err, nil, "No error on exchange bind")

        ok, err = ctx:teardown()
        test:ok(ok, "Teardown status ok")
        test:is(err, nil, "No error on teardown")

        ok, err = ctx:close()
        test:ok(ok, "Close status ok")
        test:is(err, nil, "No error on close")
    end

    local test_unbind = function(test, options)
        test:plan(11)

        test:diag("Client should be able to unbind exchange")
        local ctx = amqp.new(options)
        test:isnt(ctx, nil, 'Ctx created')

        local ok, err = ctx:connect("127.0.0.1", 5672)
        test:ok(ok, "Connect status ok")
        test:is(err, nil, "No error on connect")

        ok, err = ctx:setup()
        test:ok(ok, "Setup status ok")
        test:is(err, nil, "No error on setup")

        ok, err = amqp.exchange_unbind(ctx, {
            source = "amq.topic",
            destination = "topic.mengz",
            routing_key = "Kiwi"
        })
        test:ok(ok, "Exchange unbind status ok")
        test:is(err, nil, "No error on exchange unbind")

        ok, err = ctx:teardown()
        test:ok(ok, "Teardown status ok")
        test:is(err, nil, "No error on teardown")

        ok, err = ctx:close()
        test:ok(ok, "Close status ok")
        test:is(err, nil, "No error on close")
    end

    local test_delete = function(test, options)
        test:plan(11)

        test:diag("Client should be able to delete exchange")
        local ctx = amqp.new(options)
        test:isnt(ctx, nil, 'Ctx created')

        local ok, err = ctx:connect("127.0.0.1", 5672)
        test:ok(ok, "Connect status ok")
        test:is(err, nil, "No error on connect")

        ok, err = ctx:setup()
        test:ok(ok, "Setup status ok")
        test:is(err, nil, "No error on setup")

        ok, err = amqp.exchange_delete(ctx, {
            exchange = "topic.mengz",
        })
        test:ok(ok, "Exchange delete status ok")
        test:is(err, nil, "No error on exchange delete")

        ok, err = ctx:teardown()
        test:ok(ok, "Teardown status ok")
        test:is(err, nil, "No error on teardown")

        ok, err = ctx:close()
        test:ok(ok, "Close status ok")
        test:is(err, nil, "No error on close")
    end

    local test_delete_non_existing = function(test, options)
        test:plan(11)

        test:diag("Client should be able to delete exchange")
        local ctx = amqp.new(options)
        test:isnt(ctx, nil, 'Ctx created')

        local ok, err = ctx:connect("127.0.0.1", 5672)
        test:ok(ok, "Connect status ok")
        test:is(err, nil, "No error on connect")

        ok, err = ctx:setup()
        test:ok(ok, "Setup status ok")
        test:is(err, nil, "No error on setup")

        ok, err = amqp.exchange_delete(ctx, {
            exchange = "topic.mengz",
        })
        test:ok(ok, "Non-existing exchange delete status ok")
        test:is(err, nil, "No error on non-existing exchange delete")

        ok, err = ctx:teardown()
        test:ok(ok, "Teardown status ok")
        test:is(err, nil, "No error on teardown")

        ok, err = ctx:close()
        test:ok(ok, "Close status ok")
        test:is(err, nil, "No error on close")
    end

    local options = {
        role = "producer",
        exchange = "work.pub",
        routing_key = "work.rk",
        virtual_host = "workhost",
        ssl = false,
        user = "guest",
        password = "guest"
    }

    t:plan(5)
    t:test('declare', test_declare, options)
    t:test('bind', test_bind, options)
    t:test('unbind', test_unbind, options)
    t:test('delete', test_delete, options)
    t:test('delete non-existing', test_delete_non_existing, options)
end

local test = tap.test('amqp')
test:plan(4)
test:test('basic', test_basic)
test:test('basic_with_headers', test_basic_with_headers)
test:test('connection', test_connection)
test:test('test_exchange', test_exchange)

os.exit(test:check() == true and 0 or -1)
