# Bigdis

Bigdis is a persistent database that implements the Redis server protocol. Any Redis client can interface with it and start to use it right away. It's basically a *Redis-for-secondary-memory*.

The main feature of Bigdis is that it's very friendly with huge keys and huge values. Much friendlier than Redis itself, as the Redis author states (see the credits section).

It has no external dependencies of any kind. It gets away by simply using the comprehensive Go's standard library. Also, since it uses 1 goroutine per client connection it gets to scale to multiple cores "for free".


## Status
Bigdis is based on the OG [Bigdis](https://github.com/antirez/Bigdis) (see the credits section for further infos).

This is the subset of commands it currently implements:

|Command |Status|Comment
--- | --- | ---
|`PING`|:heavy_check_mark|
|`GET`|:heavy_check_mark|
|`SET`|:wrench:|Only setting values and *XX* or *NX* work, no keys expiration logic as of now
|`DEL`|:heavy_check_mark|
|`GETDEL`|:heavy_check_mark|
|`EXISTS`|:heavy_check_mark|
|`COMMAND`|:wrench:|Placeholder reply only
|`SELECT`|:heavy_check_mark|
|`FLUSHDB`|:heavy_check_mark:|
|`GETSET`|:heavy_check_mark:|
|`FLUSHALL`|:heavy_check_mark:|
|`STRLEN`|:heavy_check_mark:|
|`APPEND`|:heavy_check_mark:|
|`INCR`|:heavy_check_mark:|
|`INCRBY`|:heavy_check_mark:|
|`DECR`|:heavy_check_mark:|
|`DECRBY`|:heavy_check_mark:|

Nothing other than the string type has been implemented as of now.

## Credits
This project is heavily inspired - starting from its name - by the TCL lang experiment that *antirez* - the creator of Redis - did [in this repo](https://github.com/antirez/Bigdis) in July 2010. My project is an answer to the question in his README "Do you think this idea is useful?". I think it really is so I implemented it in Go.

Most parsing code of client requests and replying is taken [from here](https://github.com/r0123r/go-redis-server) to jumpstart the implementation.
