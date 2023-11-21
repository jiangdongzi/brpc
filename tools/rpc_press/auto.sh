rpc_press=/root/github/with_redis_cluster_brpc/brpc/build/output/bin/rpc_press
proto=/root/github/with_redis_cluster_brpc/brpc/example/echo_c++/echo.proto

$rpc_press -proto=$proto -method=example.EchoService.Echo -server=0.0.0.0:8000 -input=./input.json -qps=1 -duration=0 -protocol=h2:grpc -output=out.json -req_once=true
#$rpc_press -proto=$proto -method=example.EchoService.Echo -server=0.0.0.0:8000 -input=./input.json -qps=1 -duration=0 -protocol=h2:grpc -output=out.json -req_once=false
