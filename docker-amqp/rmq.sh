docker stop unit_test_rmq
docker container rm unit_test_rmq
docker run --name unit_test_rmq -d -p 5672:5672 -p 15672:15672 -v `pwd`/definitions.json:/etc/rabbitmq/definitions.json rabbitmq:3-management
sleep 8
docker exec unit_test_rmq rabbitmqctl import_definitions /etc/rabbitmq/definitions.json
docker attach unit_test_rmq
