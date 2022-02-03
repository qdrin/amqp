# AMQP
Tarantool Client for AMQP 0.9.1. Based on https://github.com/mengz0/amqp.
Thanks to the author's for the great job.

## Typical Use Cases

+ Consumer

```lua
local amqp = require("amqp")
local ctx = amqp.new({
	role = "consumer",
	queue = "work_q",
	ssl = false,
	-- if you want passive connection with no binding and queue declaring
	user = "read_user",
	password = "read_user",
	passive = false,
	-- if you have rights to declare and bind
	-- user = "guest",
	-- password = "guest",
	-- passive = true,
	-- exchange = "work.pub",
	-- routing_key = "work.rk",
	-- optional features
	consumer_tag = "test_consumer_tag", 
	properties = {
		product = "my_cute_product",
		version = "1.0",
		-- Any other properties you want
		-- Note: the platform, host, product and version properties is filled by default values: `uname -oi`, `hostname -f`, tarantool-amqp and it's version respectivly. You can change thev by your own at any moment
	}

})
ctx:connect("127.0.0.1", 5672)
local ok, err = ctx:consume()
```

+ Producer

```lua
local amqp = require("amqp")
local ctx = amqp.new({
	role = "publisher",
	exchange = "work.pub",
	routing_key = "work.rk",
	ssl = false,
	-- the same 'fork' with declaring/binding rights
	user = "guest",
	password = "guest",
	passive = false,
	-- OR
	user = "read_user",
	password = "read_user",
	passive = true,
	-- Just as in consumer, the properties SHOULD work, but not tested yet.
})
ctx:connect("127.0.0.1", 5672)
ctx:setup()
local ok, err = ctx:publish("Hello world!")
```

You can also check [test/basic.lua](test/basic.lua).

## Local testing

### Prerequisits

#### RabbitMQ

Running tests require configured `RabbitMQ` instance. You can find it in ./docker-amqp folder
Just run ./rmq.sh from there

#### docker

Naturally You need install and working docker.

#### install the rock
`tarantoolctl rocks make`

### Run tests:
`tarantool test/publisher.lua`  # publisher test
`tarantool test/consumer.lua`   # consumer test