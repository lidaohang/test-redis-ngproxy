package main

import (
	"encoding/json"
	"reflect"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/go-redis/redis"
)

var _ = Describe("Commands", func() {
	var client *redis.Client

	BeforeEach(func() {
		var options = &redis.Options{
			Addr:     "10.94.106.240:8015",
			Password: "", // no password set
			DB:       0,  // use default DB

		}

		client = redis.NewClient(options)
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	Describe("server", func() {

		It("should Ping", func() {
			ping := client.Ping()
			Expect(ping.Err()).NotTo(HaveOccurred())
			Expect(ping.Val()).To(Equal("PONG"))
		})

	})

	Describe("keys", func() {

		It("should Del", func() {
			err := client.Set("key1", "Hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.Set("key2", "World", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			n, err := client.Del("key1", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Dump", func() {
			set := client.Set("key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			dump := client.Dump("key")
			Expect(dump.Err()).NotTo(HaveOccurred())
			Expect(dump.Val()).NotTo(BeEmpty())
		})

		It("should Exists", func() {
			set := client.Set("key1", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			n, err := client.Exists("key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.Exists("key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			n, err = client.Exists("key1", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			n, err = client.Exists("key1", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(2)))
		})

		It("should Expire", func() {
			set := client.Set("key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire("key", 10*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl := client.TTL("key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(10 * time.Second))

			set = client.Set("key", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			ttl = client.TTL("key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val() < 0).To(Equal(true))
		})

		It("should ExpireAt", func() {
			set := client.Set("key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			n, err := client.Exists("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(1)))

			expireAt := client.ExpireAt("key", time.Now().Add(-time.Hour))
			Expect(expireAt.Err()).NotTo(HaveOccurred())
			Expect(expireAt.Val()).To(Equal(true))

			n, err = client.Exists("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should PExpire", func() {
			set := client.Set("key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := 900 * time.Millisecond
			pexpire := client.PExpire("key", expiration)
			Expect(pexpire.Err()).NotTo(HaveOccurred())
			Expect(pexpire.Val()).To(Equal(true))

			ttl := client.TTL("key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Second))

			pttl := client.PTTL("key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 10*time.Millisecond))
		})

		It("should PExpireAt", func() {
			set := client.Set("key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := 900 * time.Millisecond
			pexpireat := client.PExpireAt("key", time.Now().Add(expiration))
			Expect(pexpireat.Err()).NotTo(HaveOccurred())
			Expect(pexpireat.Val()).To(Equal(true))

			ttl := client.TTL("key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(time.Second))

			pttl := client.PTTL("key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 10*time.Millisecond))
		})

		It("should PTTL", func() {
			set := client.Set("key", "Hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expiration := time.Second
			expire := client.Expire("key", expiration)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			pttl := client.PTTL("key")
			Expect(pttl.Err()).NotTo(HaveOccurred())
			Expect(pttl.Val()).To(BeNumerically("~", expiration, 10*time.Millisecond))
		})

		It("should Sort", func() {
			client.Del("list").Result()

			size, err := client.LPush("list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush("list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush("list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			els, err := client.Sort("list", redis.Sort{
				Offset: 0,
				Count:  2,
				Order:  "ASC",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(els).To(Equal([]string{"1", "2"}))
		})

		It("should Sort and Get", func() {
			client.Del("list", "object_2").Result()

			size, err := client.LPush("list", "1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(1)))

			size, err = client.LPush("list", "3").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(2)))

			size, err = client.LPush("list", "2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(3)))

			err = client.Set("object_2", "value2", 0).Err()
			Expect(err).NotTo(HaveOccurred())

		})

		It("should TTL", func() {
			client.Del("key").Result()

			ttl := client.TTL("key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val() < 0).To(Equal(true))

			set := client.Set("key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			expire := client.Expire("key", 60*time.Second)
			Expect(expire.Err()).NotTo(HaveOccurred())
			Expect(expire.Val()).To(Equal(true))

			ttl = client.TTL("key")
			Expect(ttl.Err()).NotTo(HaveOccurred())
			Expect(ttl.Val()).To(Equal(60 * time.Second))
		})

		It("should Type", func() {
			set := client.Set("key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			type_ := client.Type("key")
			Expect(type_.Err()).NotTo(HaveOccurred())
			Expect(type_.Val()).To(Equal("string"))
		})

	})

	Describe("strings", func() {

		It("should Append", func() {
			client.Del("key").Result()

			n, err := client.Exists("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))

			append := client.Append("key", "Hello")
			Expect(append.Err()).NotTo(HaveOccurred())
			Expect(append.Val()).To(Equal(int64(5)))

			append = client.Append("key", " World")
			Expect(append.Err()).NotTo(HaveOccurred())
			Expect(append.Val()).To(Equal(int64(11)))

			get := client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello World"))
		})

		It("should BitCount", func() {
			set := client.Set("key", "foobar", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			bitCount := client.BitCount("key", nil)
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(26)))

			bitCount = client.BitCount("key", &redis.BitCount{0, 0})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(4)))

			bitCount = client.BitCount("key", &redis.BitCount{1, 1})
			Expect(bitCount.Err()).NotTo(HaveOccurred())
			Expect(bitCount.Val()).To(Equal(int64(6)))
		})

		It("should Decr", func() {
			client.Del("key").Result()

			set := client.Set("key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decr := client.Decr("key")
			Expect(decr.Err()).NotTo(HaveOccurred())
			Expect(decr.Val()).To(Equal(int64(9)))

			set = client.Set("key", "234293482390480948029348230948", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decr = client.Decr("key")
			Expect(decr.Err()).To(MatchError("ERR value is not an integer or out of range"))
			Expect(decr.Val()).To(Equal(int64(0)))
		})

		It("should DecrBy", func() {
			client.Del("key").Result()

			set := client.Set("key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			decrBy := client.DecrBy("key", 5)
			Expect(decrBy.Err()).NotTo(HaveOccurred())
			Expect(decrBy.Val()).To(Equal(int64(5)))
		})

		It("should Get", func() {
			get := client.Get("_")
			Expect(get.Err()).To(Equal(redis.Nil))
			Expect(get.Val()).To(Equal(""))

			set := client.Set("key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get = client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should GetBit", func() {
			setBit := client.SetBit("key", 7, 1)
			Expect(setBit.Err()).NotTo(HaveOccurred())
			Expect(setBit.Val()).To(Equal(int64(0)))

			getBit := client.GetBit("key", 0)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))

			getBit = client.GetBit("key", 7)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(1)))

			getBit = client.GetBit("key", 100)
			Expect(getBit.Err()).NotTo(HaveOccurred())
			Expect(getBit.Val()).To(Equal(int64(0)))
		})

		It("should GetRange", func() {
			set := client.Set("key", "This is a string", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			getRange := client.GetRange("key", 0, 3)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This"))

			getRange = client.GetRange("key", -3, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("ing"))

			getRange = client.GetRange("key", 0, -1)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("This is a string"))

			getRange = client.GetRange("key", 10, 100)
			Expect(getRange.Err()).NotTo(HaveOccurred())
			Expect(getRange.Val()).To(Equal("string"))
		})

		It("should GetSet", func() {
			client.Del("key").Result()

			incr := client.Incr("key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(1)))

			getSet := client.GetSet("key", "0")
			Expect(getSet.Err()).NotTo(HaveOccurred())
			Expect(getSet.Val()).To(Equal("1"))

			get := client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("0"))
		})

		It("should Incr", func() {
			set := client.Set("key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incr := client.Incr("key")
			Expect(incr.Err()).NotTo(HaveOccurred())
			Expect(incr.Val()).To(Equal(int64(11)))

			get := client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("11"))
		})

		It("should IncrBy", func() {
			set := client.Set("key", "10", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrBy := client.IncrBy("key", 5)
			Expect(incrBy.Err()).NotTo(HaveOccurred())
			Expect(incrBy.Val()).To(Equal(int64(15)))
		})

		It("should IncrByFloat", func() {
			set := client.Set("key", "10.50", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat := client.IncrByFloat("key", 0.1)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(10.6))

			set = client.Set("key", "5.0e3", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			incrByFloat = client.IncrByFloat("key", 2.0e2)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should IncrByFloatOverflow", func() {
			client.Del("key").Result()

			incrByFloat := client.IncrByFloat("key", 996945661)
			Expect(incrByFloat.Err()).NotTo(HaveOccurred())
			Expect(incrByFloat.Val()).To(Equal(float64(996945661)))
		})

		It("should MSetMGet", func() {
			mSet := client.MSet("key1", "hello1", "key2", "hello2")
			Expect(mSet.Err()).NotTo(HaveOccurred())
			Expect(mSet.Val()).To(Equal("OK"))

			mGet := client.MGet("key1", "key2", "_")
			Expect(mGet.Err()).NotTo(HaveOccurred())
			Expect(mGet.Val()).To(Equal([]interface{}{"hello1", "hello2", nil}))
		})

		It("should Set with expiration", func() {
			err := client.Set("key", "hello", 100*time.Millisecond).Err()
			Expect(err).NotTo(HaveOccurred())

			val, err := client.Get("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))

			Eventually(func() error {
				return client.Get("foo").Err()
			}, "1s", "100ms").Should(Equal(redis.Nil))
		})

		It("should SetGet", func() {
			set := client.Set("key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			get := client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetNX", func() {
			client.Del("key").Result()

			setNX := client.SetNX("key", "hello", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(true))

			setNX = client.SetNX("key", "hello2", 0)
			Expect(setNX.Err()).NotTo(HaveOccurred())
			Expect(setNX.Val()).To(Equal(false))

			get := client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("hello"))
		})

		It("should SetNX with expiration", func() {
			client.Del("key").Result()

			isSet, err := client.SetNX("key", "hello", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			isSet, err = client.SetNX("key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			val, err := client.Get("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello"))
		})

		It("should SetXX", func() {
			client.Del("key").Result()

			isSet, err := client.SetXX("key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set("key", "hello", 0).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX("key", "hello2", 0).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetXX with expiration", func() {
			client.Del("key").Result()

			isSet, err := client.SetXX("key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(false))

			err = client.Set("key", "hello", time.Second).Err()
			Expect(err).NotTo(HaveOccurred())

			isSet, err = client.SetXX("key", "hello2", time.Second).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(isSet).To(Equal(true))

			val, err := client.Get("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal("hello2"))
		})

		It("should SetRange", func() {
			set := client.Set("key", "Hello World", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			range_ := client.SetRange("key", 6, "Redis")
			Expect(range_.Err()).NotTo(HaveOccurred())
			Expect(range_.Val()).To(Equal(int64(11)))

			get := client.Get("key")
			Expect(get.Err()).NotTo(HaveOccurred())
			Expect(get.Val()).To(Equal("Hello Redis"))
		})

		It("should StrLen", func() {
			set := client.Set("key", "hello", 0)
			Expect(set.Err()).NotTo(HaveOccurred())
			Expect(set.Val()).To(Equal("OK"))

			strLen := client.StrLen("key")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(5)))

			strLen = client.StrLen("_")
			Expect(strLen.Err()).NotTo(HaveOccurred())
			Expect(strLen.Val()).To(Equal(int64(0)))
		})

	})

	Describe("hashes", func() {

		It("should HDel", func() {
			hSet := client.HSet("hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hDel := client.HDel("hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(1)))

			hDel = client.HDel("hash", "key")
			Expect(hDel.Err()).NotTo(HaveOccurred())
			Expect(hDel.Val()).To(Equal(int64(0)))
		})

		It("should HExists", func() {
			client.Del("hash").Result()

			hSet := client.HSet("hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hExists := client.HExists("hash", "key")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(true))

			hExists = client.HExists("hash", "key1")
			Expect(hExists.Err()).NotTo(HaveOccurred())
			Expect(hExists.Val()).To(Equal(false))
		})

		It("should HGet", func() {
			client.Del("hash").Result()

			hSet := client.HSet("hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hGet := client.HGet("hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))

			hGet = client.HGet("hash", "key1")
			Expect(hGet.Err()).To(Equal(redis.Nil))
			Expect(hGet.Val()).To(Equal(""))
		})

		It("should HGetAll", func() {
			client.Del("hash").Result()

			err := client.HSet("hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet("hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			m, err := client.HGetAll("hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(map[string]string{"key1": "hello1", "key2": "hello2"}))
		})

		It("should HIncrBy", func() {
			client.Del("hash").Result()

			hSet := client.HSet("hash", "key", "5")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hIncrBy := client.HIncrBy("hash", "key", 1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(6)))

			hIncrBy = client.HIncrBy("hash", "key", -1)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(5)))

			hIncrBy = client.HIncrBy("hash", "key", -10)
			Expect(hIncrBy.Err()).NotTo(HaveOccurred())
			Expect(hIncrBy.Val()).To(Equal(int64(-5)))
		})

		It("should HIncrByFloat", func() {
			client.Del("hash").Result()

			hSet := client.HSet("hash", "field", "10.50")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(true))

			hIncrByFloat := client.HIncrByFloat("hash", "field", 0.1)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(10.6))

			hSet = client.HSet("hash", "field", "5.0e3")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(false))

			hIncrByFloat = client.HIncrByFloat("hash", "field", 2.0e2)
			Expect(hIncrByFloat.Err()).NotTo(HaveOccurred())
			Expect(hIncrByFloat.Val()).To(Equal(float64(5200)))
		})

		It("should HKeys", func() {
			client.Del("hash").Result()

			hkeys := client.HKeys("hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{}))

			hset := client.HSet("hash", "key1", "hello1")
			Expect(hset.Err()).NotTo(HaveOccurred())
			hset = client.HSet("hash", "key2", "hello2")
			Expect(hset.Err()).NotTo(HaveOccurred())

			hkeys = client.HKeys("hash")
			Expect(hkeys.Err()).NotTo(HaveOccurred())
			Expect(hkeys.Val()).To(Equal([]string{"key1", "key2"}))
		})

		It("should HLen", func() {
			client.Del("hash").Result()

			hSet := client.HSet("hash", "key1", "hello1")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			hSet = client.HSet("hash", "key2", "hello2")
			Expect(hSet.Err()).NotTo(HaveOccurred())

			hLen := client.HLen("hash")
			Expect(hLen.Err()).NotTo(HaveOccurred())
			Expect(hLen.Val()).To(Equal(int64(2)))
		})

		It("should HMGet", func() {
			client.Del("hash").Result()

			err := client.HSet("hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet("hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			vals, err := client.HMGet("hash", "key1", "key2", "_").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]interface{}{"hello1", "hello2", nil}))
		})

		It("should HMSet", func() {
			client.Del("hash").Result()

			ok, err := client.HMSet("hash", map[string]interface{}{
				"key1": "hello1",
				"key2": "hello2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(Equal("OK"))

			v, err := client.HGet("hash", "key1").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello1"))

			v, err = client.HGet("hash", "key2").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal("hello2"))
		})

		It("should HSet", func() {
			client.Del("hash").Result()

			hSet := client.HSet("hash", "key", "hello")
			Expect(hSet.Err()).NotTo(HaveOccurred())
			Expect(hSet.Val()).To(Equal(true))

			hGet := client.HGet("hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))
		})

		It("should HSetNX", func() {
			client.Del("hash").Result()

			hSetNX := client.HSetNX("hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(true))

			hSetNX = client.HSetNX("hash", "key", "hello")
			Expect(hSetNX.Err()).NotTo(HaveOccurred())
			Expect(hSetNX.Val()).To(Equal(false))

			hGet := client.HGet("hash", "key")
			Expect(hGet.Err()).NotTo(HaveOccurred())
			Expect(hGet.Val()).To(Equal("hello"))
		})

		It("should HVals", func() {
			client.Del("hash").Result()

			err := client.HSet("hash", "key1", "hello1").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.HSet("hash", "key2", "hello2").Err()
			Expect(err).NotTo(HaveOccurred())

			v, err := client.HVals("hash").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(v).To(Equal([]string{"hello1", "hello2"}))

			var slice []string
			err = client.HVals("hash").ScanSlice(&slice)
			Expect(err).NotTo(HaveOccurred())
			Expect(slice).To(Equal([]string{"hello1", "hello2"}))
		})

	})

	Describe("lists", func() {

		It("should LIndex", func() {
			client.Del("list").Result()

			lPush := client.LPush("list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush("list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lIndex := client.LIndex("list", 0)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("Hello"))

			lIndex = client.LIndex("list", -1)
			Expect(lIndex.Err()).NotTo(HaveOccurred())
			Expect(lIndex.Val()).To(Equal("World"))

			lIndex = client.LIndex("list", 3)
			Expect(lIndex.Err()).To(Equal(redis.Nil))
			Expect(lIndex.Val()).To(Equal(""))
		})

		It("should LInsert", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lInsert := client.LInsert("list", "BEFORE", "World", "There")
			Expect(lInsert.Err()).NotTo(HaveOccurred())
			Expect(lInsert.Val()).To(Equal(int64(3)))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "There", "World"}))
		})

		It("should LLen", func() {
			client.Del("list").Result()

			lPush := client.LPush("list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush("list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lLen := client.LLen("list")
			Expect(lLen.Err()).NotTo(HaveOccurred())
			Expect(lLen.Val()).To(Equal(int64(2)))
		})

		It("should LPop", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lPop := client.LPop("list")
			Expect(lPop.Err()).NotTo(HaveOccurred())
			Expect(lPop.Val()).To(Equal("one"))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should LPush", func() {
			client.Del("list").Result()

			lPush := client.LPush("list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())
			lPush = client.LPush("list", "Hello")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should LPushX", func() {
			client.Del("list", "list1", "list2").Result()

			lPush := client.LPush("list", "World")
			Expect(lPush.Err()).NotTo(HaveOccurred())

			lPushX := client.LPushX("list", "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(2)))

			lPushX = client.LPushX("list2", "Hello")
			Expect(lPushX.Err()).NotTo(HaveOccurred())
			Expect(lPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange("list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRange", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRange := client.LRange("list", 0, 0)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one"}))

			lRange = client.LRange("list", -3, 2)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange("list", -100, 100)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two", "three"}))

			lRange = client.LRange("list", 5, 10)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

		It("should LRem", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "key")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lRem := client.LRem("list", -2, "hello")
			Expect(lRem.Err()).NotTo(HaveOccurred())
			Expect(lRem.Val()).To(Equal(int64(2)))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"hello", "key"}))
		})

		It("should LSet", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lSet := client.LSet("list", 0, "four")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lSet = client.LSet("list", -2, "five")
			Expect(lSet.Err()).NotTo(HaveOccurred())
			Expect(lSet.Val()).To(Equal("OK"))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"four", "five", "three"}))
		})

		It("should LTrim", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			lTrim := client.LTrim("list", 1, -1)
			Expect(lTrim.Err()).NotTo(HaveOccurred())
			Expect(lTrim.Val()).To(Equal("OK"))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should RPop", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "one")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "two")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			rPush = client.RPush("list", "three")
			Expect(rPush.Err()).NotTo(HaveOccurred())

			rPop := client.RPop("list")
			Expect(rPop.Err()).NotTo(HaveOccurred())
			Expect(rPop.Val()).To(Equal("three"))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"one", "two"}))
		})

		It("should RPush", func() {
			client.Del("list").Result()

			rPush := client.RPush("list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPush = client.RPush("list", "World")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(2)))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))
		})

		It("should RPushX", func() {
			client.Del("list", "list2").Result()

			rPush := client.RPush("list", "Hello")
			Expect(rPush.Err()).NotTo(HaveOccurred())
			Expect(rPush.Val()).To(Equal(int64(1)))

			rPushX := client.RPushX("list", "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(2)))

			rPushX = client.RPushX("list2", "World")
			Expect(rPushX.Err()).NotTo(HaveOccurred())
			Expect(rPushX.Val()).To(Equal(int64(0)))

			lRange := client.LRange("list", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{"Hello", "World"}))

			lRange = client.LRange("list2", 0, -1)
			Expect(lRange.Err()).NotTo(HaveOccurred())
			Expect(lRange.Val()).To(Equal([]string{}))
		})

	})

	Describe("sets", func() {

		It("should SAdd", func() {
			client.Del("set").Result()

			sAdd := client.SAdd("set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd("set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd("set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(0)))

			sMembers := client.SMembers("set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SCard", func() {
			client.Del("set").Result()

			sAdd := client.SAdd("set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sAdd = client.SAdd("set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			Expect(sAdd.Val()).To(Equal(int64(1)))

			sCard := client.SCard("set")
			Expect(sCard.Err()).NotTo(HaveOccurred())
			Expect(sCard.Val()).To(Equal(int64(2)))
		})

		It("should IsMember", func() {
			client.Del("set").Result()

			sAdd := client.SAdd("set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sIsMember := client.SIsMember("set", "one")
			Expect(sIsMember.Err()).NotTo(HaveOccurred())
			Expect(sIsMember.Val()).To(Equal(true))

			sIsMember = client.SIsMember("set", "two")
			Expect(sIsMember.Err()).NotTo(HaveOccurred())
			Expect(sIsMember.Val()).To(Equal(false))
		})

		It("should SMembers", func() {
			client.Del("set").Result()

			sAdd := client.SAdd("set", "Hello")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd("set", "World")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sMembers := client.SMembers("set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"Hello", "World"}))
		})

		It("should SPop", func() {
			client.Del("set").Result()

			sAdd := client.SAdd("set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd("set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd("set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sPop := client.SPop("set")
			Expect(sPop.Err()).NotTo(HaveOccurred())
			Expect(sPop.Val()).NotTo(Equal(""))

			sMembers := client.SMembers("set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(HaveLen(2))

		})

		It("should SRandMember and SRandMemberN", func() {
			client.Del("set").Result()

			err := client.SAdd("set", "one").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.SAdd("set", "two").Err()
			Expect(err).NotTo(HaveOccurred())
			err = client.SAdd("set", "three").Err()
			Expect(err).NotTo(HaveOccurred())

			members, err := client.SMembers("set").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(HaveLen(3))

			member, err := client.SRandMember("set").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(member).NotTo(Equal(""))

			members, err = client.SRandMemberN("set", 2).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(members).To(HaveLen(2))
		})

		It("should SRem", func() {
			client.Del("set").Result()

			sAdd := client.SAdd("set", "one")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd("set", "two")
			Expect(sAdd.Err()).NotTo(HaveOccurred())
			sAdd = client.SAdd("set", "three")
			Expect(sAdd.Err()).NotTo(HaveOccurred())

			sRem := client.SRem("set", "one")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(1)))

			sRem = client.SRem("set", "four")
			Expect(sRem.Err()).NotTo(HaveOccurred())
			Expect(sRem.Val()).To(Equal(int64(0)))

			sMembers := client.SMembers("set")
			Expect(sMembers.Err()).NotTo(HaveOccurred())
			Expect(sMembers.Val()).To(ConsistOf([]string{"three", "two"}))
		})

	})

	Describe("sorted sets", func() {

		It("should ZAdd", func() {
			client.Del("zset").Result()

			added, err := client.ZAdd("zset", redis.Z{1, "one"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd("zset", redis.Z{1, "uno"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd("zset", redis.Z{2, "two"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd("zset", redis.Z{3, "two"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			vals, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{1, "one"}, {1, "uno"}, {3, "two"}}))
		})

		It("should ZAdd bytes", func() {
			client.Del("zset").Result()

			added, err := client.ZAdd("zset", redis.Z{1, []byte("one")}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd("zset", redis.Z{1, []byte("uno")}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd("zset", redis.Z{2, []byte("two")}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(1)))

			added, err = client.ZAdd("zset", redis.Z{3, []byte("two")}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(added).To(Equal(int64(0)))

			val, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{1, "one"}, {1, "uno"}, {3, "two"}}))
		})

		It("should ZIncr", func() {
			client.Del("zset").Result()

			score, err := client.ZIncr("zset", redis.Z{1, "one"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(score).To(Equal(float64(1)))

			vals, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{1, "one"}}))

			score, err = client.ZIncr("zset", redis.Z{1, "one"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(score).To(Equal(float64(2)))

			vals, err = client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{2, "one"}}))
		})

		It("should ZCard", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zCard := client.ZCard("zset")
			Expect(zCard.Err()).NotTo(HaveOccurred())
			Expect(zCard.Val()).To(Equal(int64(2)))
		})

		It("should ZCount", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zCount := client.ZCount("zset", "-inf", "+inf")
			Expect(zCount.Err()).NotTo(HaveOccurred())
			Expect(zCount.Val()).To(Equal(int64(3)))

			zCount = client.ZCount("zset", "(1", "3")
			Expect(zCount.Err()).NotTo(HaveOccurred())
			Expect(zCount.Val()).To(Equal(int64(2)))
		})

		It("should ZIncrBy", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zIncrBy := client.ZIncrBy("zset", 2, "one")
			Expect(zIncrBy.Err()).NotTo(HaveOccurred())
			Expect(zIncrBy.Val()).To(Equal(float64(3)))

			val, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{2, "two"}, {3, "one"}}))
		})

		It("should ZRange", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRange := client.ZRange("zset", 0, -1)
			Expect(zRange.Err()).NotTo(HaveOccurred())
			Expect(zRange.Val()).To(Equal([]string{"one", "two", "three"}))

			zRange = client.ZRange("zset", 2, 3)
			Expect(zRange.Err()).NotTo(HaveOccurred())
			Expect(zRange.Val()).To(Equal([]string{"three"}))

			zRange = client.ZRange("zset", -2, -1)
			Expect(zRange.Err()).NotTo(HaveOccurred())
			Expect(zRange.Val()).To(Equal([]string{"two", "three"}))
		})

		It("should ZRangeWithScores", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			val, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{1, "one"}, {2, "two"}, {3, "three"}}))

			val, err = client.ZRangeWithScores("zset", 2, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{3, "three"}}))

			val, err = client.ZRangeWithScores("zset", -2, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{2, "two"}, {3, "three"}}))
		})

		It("should ZRangeByScore", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRangeByScore := client.ZRangeByScore("zset", redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{"one", "two", "three"}))

			zRangeByScore = client.ZRangeByScore("zset", redis.ZRangeBy{
				Min: "1",
				Max: "2",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{"one", "two"}))

			zRangeByScore = client.ZRangeByScore("zset", redis.ZRangeBy{
				Min: "(1",
				Max: "2",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{"two"}))

			zRangeByScore = client.ZRangeByScore("zset", redis.ZRangeBy{
				Min: "(1",
				Max: "(2",
			})
			Expect(zRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRangeByScore.Val()).To(Equal([]string{}))
		})

		It("should ZRangeByLex", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{0, "a"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{0, "b"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{0, "c"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRangeByLex := client.ZRangeByLex("zset", redis.ZRangeBy{
				Min: "-",
				Max: "+",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{"a", "b", "c"}))

			zRangeByLex = client.ZRangeByLex("zset", redis.ZRangeBy{
				Min: "[a",
				Max: "[b",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{"a", "b"}))

			zRangeByLex = client.ZRangeByLex("zset", redis.ZRangeBy{
				Min: "(a",
				Max: "[b",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{"b"}))

			zRangeByLex = client.ZRangeByLex("zset", redis.ZRangeBy{
				Min: "(a",
				Max: "(b",
			})
			Expect(zRangeByLex.Err()).NotTo(HaveOccurred())
			Expect(zRangeByLex.Val()).To(Equal([]string{}))
		})

		It("should ZRangeByScoreWithScoresMap", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			val, err := client.ZRangeByScoreWithScores("zset", redis.ZRangeBy{
				Min: "-inf",
				Max: "+inf",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{1, "one"}, {2, "two"}, {3, "three"}}))

			val, err = client.ZRangeByScoreWithScores("zset", redis.ZRangeBy{
				Min: "1",
				Max: "2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{1, "one"}, {2, "two"}}))

			val, err = client.ZRangeByScoreWithScores("zset", redis.ZRangeBy{
				Min: "(1",
				Max: "2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{2, "two"}}))

			val, err = client.ZRangeByScoreWithScores("zset", redis.ZRangeBy{
				Min: "(1",
				Max: "(2",
			}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{}))
		})

		It("should ZRank", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRank := client.ZRank("zset", "three")
			Expect(zRank.Err()).NotTo(HaveOccurred())
			Expect(zRank.Val()).To(Equal(int64(2)))

			zRank = client.ZRank("zset", "four")
			Expect(zRank.Err()).To(Equal(redis.Nil))
			Expect(zRank.Val()).To(Equal(int64(0)))
		})

		It("should ZRem", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRem := client.ZRem("zset", "two")
			Expect(zRem.Err()).NotTo(HaveOccurred())
			Expect(zRem.Val()).To(Equal(int64(1)))

			val, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{1, "one"}, {3, "three"}}))
		})

		It("should ZRemRangeByRank", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRemRangeByRank := client.ZRemRangeByRank("zset", 0, 1)
			Expect(zRemRangeByRank.Err()).NotTo(HaveOccurred())
			Expect(zRemRangeByRank.Val()).To(Equal(int64(2)))

			val, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{3, "three"}}))
		})

		It("should ZRemRangeByScore", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRemRangeByScore := client.ZRemRangeByScore("zset", "-inf", "(2")
			Expect(zRemRangeByScore.Err()).NotTo(HaveOccurred())
			Expect(zRemRangeByScore.Val()).To(Equal(int64(1)))

			val, err := client.ZRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{2, "two"}, {3, "three"}}))
		})

		It("should ZRemRangeByLex", func() {
			client.Del("zset").Result()

			zz := []redis.Z{
				{0, "aaaa"},
				{0, "b"},
				{0, "c"},
				{0, "d"},
				{0, "e"},
				{0, "foo"},
				{0, "zap"},
				{0, "zip"},
				{0, "ALPHA"},
				{0, "alpha"},
			}
			for _, z := range zz {
				err := client.ZAdd("zset", z).Err()
				Expect(err).NotTo(HaveOccurred())
			}

			n, err := client.ZRemRangeByLex("zset", "[alpha", "[omega").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(6)))

			vals, err := client.ZRange("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"ALPHA", "aaaa", "zap", "zip"}))
		})

		It("should ZRevRange", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRevRange := client.ZRevRange("zset", 0, -1)
			Expect(zRevRange.Err()).NotTo(HaveOccurred())
			Expect(zRevRange.Val()).To(Equal([]string{"three", "two", "one"}))

			zRevRange = client.ZRevRange("zset", 2, 3)
			Expect(zRevRange.Err()).NotTo(HaveOccurred())
			Expect(zRevRange.Val()).To(Equal([]string{"one"}))

			zRevRange = client.ZRevRange("zset", -2, -1)
			Expect(zRevRange.Err()).NotTo(HaveOccurred())
			Expect(zRevRange.Val()).To(Equal([]string{"two", "one"}))
		})

		It("should ZRevRangeWithScoresMap", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			val, err := client.ZRevRangeWithScores("zset", 0, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{3, "three"}, {2, "two"}, {1, "one"}}))

			val, err = client.ZRevRangeWithScores("zset", 2, 3).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{1, "one"}}))

			val, err = client.ZRevRangeWithScores("zset", -2, -1).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{2, "two"}, {1, "one"}}))
		})

		It("should ZRevRangeByScore", func() {
			client.Del("zset").Result()

			zadd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zadd.Err()).NotTo(HaveOccurred())
			zadd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zadd.Err()).NotTo(HaveOccurred())
			zadd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zadd.Err()).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByScore(
				"zset", redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"three", "two", "one"}))

			vals, err = client.ZRevRangeByScore(
				"zset", redis.ZRangeBy{Max: "2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"two"}))

			vals, err = client.ZRevRangeByScore(
				"zset", redis.ZRangeBy{Max: "(2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{}))
		})

		It("should ZRevRangeByLex", func() {
			client.Del("zset").Result()

			zadd := client.ZAdd("zset", redis.Z{0, "a"})
			Expect(zadd.Err()).NotTo(HaveOccurred())
			zadd = client.ZAdd("zset", redis.Z{0, "b"})
			Expect(zadd.Err()).NotTo(HaveOccurred())
			zadd = client.ZAdd("zset", redis.Z{0, "c"})
			Expect(zadd.Err()).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByLex(
				"zset", redis.ZRangeBy{Max: "+", Min: "-"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"c", "b", "a"}))

			vals, err = client.ZRevRangeByLex(
				"zset", redis.ZRangeBy{Max: "[b", Min: "(a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{"b"}))

			vals, err = client.ZRevRangeByLex(
				"zset", redis.ZRangeBy{Max: "(b", Min: "(a"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]string{}))
		})

		It("should ZRevRangeByScoreWithScores", func() {
			client.Del("zset").Result()

			zadd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zadd.Err()).NotTo(HaveOccurred())
			zadd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zadd.Err()).NotTo(HaveOccurred())
			zadd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zadd.Err()).NotTo(HaveOccurred())

			vals, err := client.ZRevRangeByScoreWithScores(
				"zset", redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(vals).To(Equal([]redis.Z{{3, "three"}, {2, "two"}, {1, "one"}}))
		})

		It("should ZRevRangeByScoreWithScoresMap", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			val, err := client.ZRevRangeByScoreWithScores(
				"zset", redis.ZRangeBy{Max: "+inf", Min: "-inf"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{3, "three"}, {2, "two"}, {1, "one"}}))

			val, err = client.ZRevRangeByScoreWithScores(
				"zset", redis.ZRangeBy{Max: "2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{{2, "two"}}))

			val, err = client.ZRevRangeByScoreWithScores(
				"zset", redis.ZRangeBy{Max: "(2", Min: "(1"}).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal([]redis.Z{}))
		})

		It("should ZRevRank", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{2, "two"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())
			zAdd = client.ZAdd("zset", redis.Z{3, "three"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zRevRank := client.ZRevRank("zset", "one")
			Expect(zRevRank.Err()).NotTo(HaveOccurred())
			Expect(zRevRank.Val()).To(Equal(int64(2)))

			zRevRank = client.ZRevRank("zset", "four")
			Expect(zRevRank.Err()).To(Equal(redis.Nil))
			Expect(zRevRank.Val()).To(Equal(int64(0)))
		})

		It("should ZScore", func() {
			client.Del("zset").Result()

			zAdd := client.ZAdd("zset", redis.Z{1.001, "one"})
			Expect(zAdd.Err()).NotTo(HaveOccurred())

			zScore := client.ZScore("zset", "one")
			Expect(zScore.Err()).NotTo(HaveOccurred())
			Expect(zScore.Val()).To(Equal(float64(1.001)))
		})

	})

	Describe("marshaling/unmarshaling", func() {

		type convTest struct {
			value  interface{}
			wanted string
			dest   interface{}
		}

		convTests := []convTest{
			{nil, "", nil},
			{"hello", "hello", new(string)},
			{[]byte("hello"), "hello", new([]byte)},
			{int(1), "1", new(int)},
			{int8(1), "1", new(int8)},
			{int16(1), "1", new(int16)},
			{int32(1), "1", new(int32)},
			{int64(1), "1", new(int64)},
			{uint(1), "1", new(uint)},
			{uint8(1), "1", new(uint8)},
			{uint16(1), "1", new(uint16)},
			{uint32(1), "1", new(uint32)},
			{uint64(1), "1", new(uint64)},
			{float32(1.0), "1", new(float32)},
			{float64(1.0), "1", new(float64)},
			{true, "1", new(bool)},
			{false, "0", new(bool)},
		}

		It("should convert to string", func() {
			for _, test := range convTests {
				err := client.Set("key", test.value, 0).Err()
				Expect(err).NotTo(HaveOccurred())

				s, err := client.Get("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(s).To(Equal(test.wanted))

				if test.dest == nil {
					continue
				}

				err = client.Get("key").Scan(test.dest)
				Expect(err).NotTo(HaveOccurred())
				Expect(deref(test.dest)).To(Equal(test.value))
			}
		})

	})

	Describe("json marshaling/unmarshaling", func() {

		BeforeEach(func() {
			value := &numberStruct{Number: 42}
			err := client.Set("key", value, 0).Err()
			Expect(err).NotTo(HaveOccurred())
		})

		It("should marshal custom values using json", func() {
			s, err := client.Get("key").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(s).To(Equal(`{"Number":42}`))
		})

		It("should scan custom values using json", func() {
			value := &numberStruct{}
			err := client.Get("key").Scan(value)
			Expect(err).NotTo(HaveOccurred())
			Expect(value.Number).To(Equal(42))
		})

	})

})

type numberStruct struct {
	Number int
}

func (s *numberStruct) MarshalBinary() ([]byte, error) {
	return json.Marshal(s)
}

func (s *numberStruct) UnmarshalBinary(b []byte) error {
	return json.Unmarshal(b, s)
}

func deref(viface interface{}) interface{} {
	v := reflect.ValueOf(viface)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	return v.Interface()
}
