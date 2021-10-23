package = 'amqp'
version = 'scm-1'

source  = {
    url    = '...';
    branch = 'my-amqp';
}

description = {
    summary  = "AMQP module for tarantool";
}

dependencies = {
    'lua >= 5.1';
}


build = {
   type = "builtin",
   modules = {
        ['amqp'] = 'amqp/init.lua',
        ['amqp.buffer'] = 'amqp/buffer.lua',
        ['amqp.consts'] = 'amqp/consts.lua',
        ['amqp.frame'] = 'amqp/frame.lua'
	}
}
