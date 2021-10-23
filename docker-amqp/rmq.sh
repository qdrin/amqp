docker stop test_rmq
docker container rm test_rmq
docker run --name test_rmq -d -p 5672:5672 -p 15672:15672 -v `pwd`/definitions.json:/etc/rabbitmq/definitions.json rabbitmq:3-management
sleep 7
docker exec test_rmq rabbitmqctl import_definitions /etc/rabbitmq/definitions.json
docker attach test_rmq
