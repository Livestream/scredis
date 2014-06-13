/*
 * Copyright (c) 2013 Livestream LLC. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package scredis.commands

/**
 * This object lists the names of all available commands.
 */
private[scredis] object Names {

  // Connection commands
  val Auth = "AUTH"
  val Echo = "ECHO"
  val Ping = "PING"
  val Quit = "QUIT"
  val Select = "SELECT"

  // Server commands
  val BgRewriteAOF = "BGREWRITEAOF"
  val BgSave = "BGSAVE"

  val Client = "CLIENT"
  val ClientGetName = "GETNAME"
  val ClientKill = "KILL"
  val ClientList = "LIST"
  val ClientSetName = "SETNAME"

  val Config = "CONFIG"
  val ConfigGet = "GET"
  val ConfigResetStat = "RESETSTAT"
  val ConfigSet = "SET"

  val DbSize = "DBSIZE"
  val FlushAll = "FLUSHALL"
  val FlushDb = "FLUSHDB"
  val Info = "INFO"
  val LastSave = "LASTSAVE"
  val Save = "SAVE"
  val Time = "TIME"

  // Keys commands
  val Del = "DEL"
  val Dump = "DUMP"
  val Exists = "EXISTS"
  val Expire = "EXPIRE"
  val ExpireAt = "EXPIREAT"
  val Keys = "KEYS"
  val Migrate = "MIGRATE"
  val Move = "MOVE"
  val Persist = "PERSIST"
  val PExpire = "PEXPIRE"
  val PExpireAt = "PEXPIREAT"
  val PTtl = "PTTL"
  val RandomKey = "RANDOMKEY"
  val Rename = "RENAME"
  val RenameNX = "RENAMENX"
  val Restore = "RESTORE"
  val Sort = "SORT"
  val Ttl = "TTL"
  val Type = "TYPE"
  val Scan = "SCAN"

  // Strings commands
  val Append = "APPEND"
  val BitCount = "BITCOUNT"
  val BitOp = "BITOP"
  val Decr = "DECR"
  val DecrBy = "DECRBY"
  val Get = "GET"
  val GetBit = "GETBIT"
  val Substr = "SUBSTR"
  val GetRange = "GETRANGE"
  val GetSet = "GETSET"
  val Incr = "INCR"
  val IncrBy = "INCRBY"
  val IncrByFloat = "INCRBYFLOAT"
  val MGet = "MGET"
  val MSet = "MSET"
  val MSetNX = "MSETNX"
  val PSetEX = "PSETEX"
  val Set = "SET"
  val SetBit = "SETBIT"
  val SetEX = "SETEX"
  val SetNX = "SETNX"
  val SetRange = "SETRANGE"
  val StrLen = "STRLEN"

  // Hashes commands
  val HDel = "HDEL"
  val HExists = "HEXISTS"
  val HGet = "HGET"
  val HGetAll = "HGETALL"
  val HIncrBy = "HINCRBY"
  val HIncrByFloat = "HINCRBYFLOAT"
  val HKeys = "HKEYS"
  val HLen = "HLEN"
  val HMGet = "HMGET"
  val HMSet = "HMSET"
  val HSet = "HSET"
  val HSetNX = "HSETNX"
  val HVals = "HVALS"
  val HScan = "HSCAN"

  // Lists commands
  val BLPop = "BLPOP"
  val BRPop = "BRPOP"
  val BRPopLPush = "BRPOPLPUSH"
  val LIndex = "LINDEX"
  val LInsert = "LINSERT"
  val LLen = "LLEN"
  val LPop = "LPOP"
  val LPush = "LPUSH"
  val LPushX = "LPUSHX"
  val LRange = "LRANGE"
  val LRem = "LREM"
  val LSet = "LSET"
  val LTrim = "LTRIM"
  val RPop = "RPOP"
  val RPopLPush = "RPOPLPUSH"
  val RPush = "RPUSH"
  val RPushX = "RPUSHX"

  // Sets commands
  val SAdd = "SADD"
  val SCard = "SCARD"
  val SDiff = "SDIFF"
  val SDiffStore = "SDIFFSTORE"
  val SInter = "SINTER"
  val SInterStore = "SINTERSTORE"
  val SIsMember = "SISMEMBER"
  val SMembers = "SMEMBERS"
  val SMove = "SMOVE"
  val SPop = "SPOP"
  val SRandMember = "SRANDMEMBER"
  val SRem = "SREM"
  val SUnion = "SUNION"
  val SUnionStore = "SUNIONSTORE"
  val SScan = "SSCAN"

  // Sorted sets commands
  val ZAdd = "ZADD"
  val ZCard = "ZCARD"
  val ZCount = "ZCOUNT"
  val ZIncrBy = "ZINCRBY"
  val ZInterStore = "ZINTERSTORE"
  val ZRange = "ZRANGE"
  val ZRangeByScore = "ZRANGEBYSCORE"
  val ZRank = "ZRANK"
  val ZRem = "ZREM"
  val ZRemRangeByRank = "ZREMRANGEBYRANK"
  val ZRemRangeByScore = "ZREMRANGEBYSCORE"
  val ZRevRange = "ZREVRANGE"
  val ZRevRangeByScore = "ZREVRANGEBYSCORE"
  val ZRevRank = "ZREVRANK"
  val ZScore = "ZSCORE"
  val ZUnionStore = "ZUNIONSTORE"
  val ZScan = "ZSCAN"

  // Scripting commands
  val Eval = "EVAL"
  val EvalSha = "EVALSHA"
  val Script = "SCRIPT"
  val ScriptExists = "EXISTS"
  val ScriptFlush = "FLUSH"
  val ScriptKill = "KILL"
  val ScriptLoad = "LOAD"

  // Pub/Sub commands
  val Publish = "PUBLISH"
  val Subscribe = "SUBSCRIBE"
  val PSubscribe = "PSUBSCRIBE"
  val Unsubscribe = "UNSUBSCRIBE"
  val PUnsubscribe = "PUNSUBSCRIBE"

  // Transactional commands
  val Multi = "MULTI"
  val Exec = "EXEC"
  val Discard = "DISCARD"
  val Watch = "WATCH"
  val UnWatch = "UNWATCH"

}