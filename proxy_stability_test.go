package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis"
	logging "github.com/op/go-logging"
)

const (
	proxyAddr  = "10.94.106.240:8015"
	masterAddr = "127.0.0.1:8001"
	slaveAddr  = "127.0.0.1:8002"
)

var format = logging.MustStringFormatter(
	`%{color}%{time:2006-01-02T15:04:05.000} %{shortfile} %{shortfunc} > %{level:.4s} %{id:03x}%{color:reset} [%{module}] %{message}`,
)

// GetLogger ...
func getLogger(path string, flag string) (*logging.Logger, error) {
	logger := logging.MustGetLogger(flag)

	logFile, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(err)
		return nil, err

	}

	backend := logging.NewLogBackend(logFile, "", 0)
	logging.SetFormatter(format)

	backendLeveled := logging.AddModuleLevel(backend)
	backendLeveled.SetLevel(logging.DEBUG, "")

	logger.SetBackend(backendLeveled)

	return logger, nil

}

func getRedisClient(addr string, poolSize int) *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     poolSize,
	})

	return client
}

/*
压测GET,SET一分钟
*/
func BenchmarkRedisNormal(b *testing.B) {

	logger, _ := getLogger("benchmark_redis_normal.log", "redis_normal")

	client := getRedisClient(proxyAddr, 10)
	defer client.Close()

	value := bytes.Repeat([]byte{'1'}, 32)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			logger.Debugf(proxyAddr, " set key value")
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
				logger.Error(err)
			}

			logger.Debugf(proxyAddr, " get key")
			got, err := client.Get("key").Bytes()
			if err != nil {
				b.Fatal(err)
				logger.Error(err)
			}
			if !bytes.Equal(got, value) {
				b.Fatalf("got != value")
				logger.Error(fmt.Sprintf("got=[%s] != value=[%s]", string(got), string(value)))
			}
		}
	})
}

/*
压测GET,SET一分钟然后下掉master节点
*/
func BenchmarkRedisMasterShutDown(b *testing.B) {

	logger, _ := getLogger("benchmark_redis_master_shutdown.log", "master_down")

	client := getRedisClient(proxyAddr, 10)
	defer client.Close()

	clientMaster := getRedisClient(masterAddr, 10)
	defer client.Close()

	now := time.Now()
	status := true

	value := bytes.Repeat([]byte{'1'}, 32)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			ti := time.Now()
			subM := ti.Sub(now)
			minutes := subM.Minutes()

			if int(minutes) >= 1 && status {
				clientMaster.Shutdown()
				status = false
			}

			logger.Debugf(proxyAddr, " set key value")
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
				logger.Error(err)
			}

			logger.Debugf(proxyAddr, " get key")
			got, err := client.Get("key").Bytes()
			if err != nil {
				b.Fatal(err)
				logger.Error(err)
			}
			if !bytes.Equal(got, value) {
				b.Fatalf("got != value")
				logger.Error(fmt.Sprintf("got=[%s] != value=[%s]", string(got), string(value)))
			}
		}
	})
}

/*
压测GET,SET一分钟然后下掉slave节点
*/
func BenchmarkRedisSlaveShutDown(b *testing.B) {

	logger, _ := getLogger("benchmark_redis_slave_shutdown.log", "slave_down")

	client := getRedisClient(proxyAddr, 10)
	defer client.Close()

	clientSlave := getRedisClient(slaveAddr, 10)
	defer client.Close()

	now := time.Now()
	status := true

	value := bytes.Repeat([]byte{'1'}, 32)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			ti := time.Now()
			subM := ti.Sub(now)
			minutes := subM.Minutes()

			if int(minutes) >= 1 && status {
				clientSlave.Shutdown()
				status = false
			}

			logger.Debugf(proxyAddr, " set key value")
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
				logger.Error(err)
			}

			logger.Debugf(proxyAddr, " get key")
			got, err := client.Get("key").Bytes()
			if err != nil {
				b.Fatal(err)
				logger.Error(err)
			}
			if !bytes.Equal(got, value) {
				b.Fatalf("got != value")
				logger.Error(fmt.Sprintf("got=[%s] != value=[%s]", string(got), string(value)))
			}
		}
	})
}

/*
压测GET,SET一分钟然后下掉master,slave节点
*/
func BenchmarkRedisMasterSlaveShutDown(b *testing.B) {

	logger, _ := getLogger("benchmark_redis_master_shutdown.log", "master_down")

	client := getRedisClient(proxyAddr, 10)
	defer client.Close()

	clientMaster := getRedisClient(masterAddr, 10)
	defer client.Close()

	clientSlave := getRedisClient(slaveAddr, 10)
	defer client.Close()

	now := time.Now()
	status := true

	value := bytes.Repeat([]byte{'1'}, 32)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {

			ti := time.Now()
			subM := ti.Sub(now)
			minutes := subM.Minutes()

			if int(minutes) >= 1 && status {
				clientMaster.Shutdown()
				clientSlave.Shutdown()
				status = false
			}

			logger.Debugf(proxyAddr, " set key value")
			if err := client.Set("key", value, 0).Err(); err != nil {
				b.Fatal(err)
				logger.Error(err)
			}

			logger.Debugf(proxyAddr, " get key")
			got, err := client.Get("key").Bytes()
			if err != nil {
				b.Fatal(err)
				logger.Error(err)
			}
			if !bytes.Equal(got, value) {
				b.Fatalf("got != value")
				logger.Error(fmt.Sprintf("got=[%s] != value=[%s]", string(got), string(value)))
			}
		}
	})
}
