all:
	go test -ginkgo.v
	go test ./... -test.run=NONE -test.bench=. -test.benchmem -ginkgo.v

bootstrap:
	ginkgo bootstrap
