all:
	go test -ginkgo.v
	go test -test.run=NONE -test.bench=. -test.benchmem

unit:
	go test -ginkgo.v

bench:
	go test -test.run=NONE -test.bench=. -test.benchmem -test.benchtime 60s


ping:
	go test -test.run=NONE -test.bench="BenchmarkRedisPing" -test.benchmem -test.benchtime 60s

getset:
	go test -test.run=NONE -test.bench="BenchmarkRedisSetGetBytes" -test.benchmem -test.benchtime 60s

bigkey:
	go test -test.run=NONE -test.bench="BenchmarkSetRedis10Conns64Bytes" -test.benchmem -test.benchtime 60s

mget:
	go test -test.run=NONE -test.bench="BenchmarkRedisMGet" -test.benchmem -test.benchtime 60s

pipeling:
	go test -test.run=NONE -test.bench="BenchmarkPipeline" -test.benchmem -test.benchtime 60s

zadd:
	go test -test.run=NONE -test.bench="BenchmarkZAdd" -test.benchmem -test.benchtime 60s


masterdown:
	go test -test.run=NONE -test.bench="BenchmarkRedisMasterShutDown" -test.benchmem -test.benchtime 300s


redisnormal:
	go test -test.run=NONE -test.bench="BenchmarkRedisNormal" -test.benchmem -test.benchtime 300s


slavedown:
	go test -test.run=NONE -test.bench="BenchmarkRedisSlaveShutDown" -test.benchmem -test.benchtime 300s


bootstrap:
	ginkgo bootstrap
