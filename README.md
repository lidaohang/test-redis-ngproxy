### redis cluster ngproxy的测试框架


#### 说明
 - 功能性单元测试
 - 非稳定性测试
 - 性能测试


#### 单元测试
 ```
 make unit
 ```

#### 性能测试
- 默认10个线程并发，循环执行5次

##### bench all
 ```
make bench
 ```

##### bench ping
```
make ping
```


##### bench getset
```
make getset
```

##### bench bigkey
```
make bigkey
```

##### bench mget
```
make mget
```

##### bench pipeling
```
make pipeling
```

##### bench zadd
```
make zadd
```
