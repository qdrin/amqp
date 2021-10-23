# AMQP
Tarantool Client for AMQP 0.9.1. Based on https://github.com/mengz0/amqp.

## Typical Use Cases

+ Consumer

```lua
local amqp = require("amqp")
local ctx = amqp.new({
	role = "consumer",
	queue = "mengz0",
	exchange = "amq.topic",
	ssl = false,
	user = "guest",
	password = "guest"
})
ctx:connect("127.0.0.1", 5672)
local ok, err = ctx:consume()
```

+ Producer

```lua
local amqp = require("amqp")
local ctx = amqp.new({
	role = "publisher",
	exchange = "amq.topic",
	ssl = false,
	user = "guest",
	password = "guest"
})
ctx:connect("127.0.0.1", 5672)
ctx:setup()
local ok, err = ctx:publish("Hello world!")
```

You can also check [test/basic.lua](test/basic.lua).

## Local testing

Running tests require some `RabbitMQ` instance available at `localhost:5672` with default credentials `guest:guest`. For example, you can run
```bash
docker run -d --hostname my-rabbit --name some-rabbit -p 5672:5672 rabbitmq:3
```
