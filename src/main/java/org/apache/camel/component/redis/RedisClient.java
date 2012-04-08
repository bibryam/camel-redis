package org.apache.camel.component.redis;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.query.SortQuery;
import org.springframework.data.redis.core.query.SortQueryBuilder;

public class RedisClient {
    private final RedisTemplate<String, String> redisTemplate;

    public RedisClient(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public void set(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    public void hmset(String key, HashMap param) {
        redisTemplate.opsForHash().putAll(key, param);
    }

    public Collection<String> hmget(String key, Collection<String> fields) {
        return redisTemplate.<String, String>opsForHash().multiGet(key, fields);
    }

    public Set<String> hkeys(String key) {
        return redisTemplate.<String, String>opsForHash().keys(key);
    }

    public Long hlen(String key) {
        return redisTemplate.opsForHash().size(key);
    }

    public Long hincrBy(String key, String field, Long value) {
        return redisTemplate.opsForHash().increment(key, field, value);
    }

    public Map<String, String> hgetAll(String key) {
        return redisTemplate.<String, String>opsForHash().entries(key);
    }

    public Boolean hexists(String key, String field) {
        return redisTemplate.opsForHash().hasKey(key, field);
    }

    public String hget(String key, String field) {
        return redisTemplate.<String, String>opsForHash().get(key, field);
    }

    public void hdel(String key, String field) {
        redisTemplate.opsForHash().delete(key, field);
    }

    public void hset(String key, String field, String value) {
        redisTemplate.opsForHash().put(key, field, value);
    }

    public void quit() {
        redisTemplate.execute(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                connection.close();
                return null;
            }
        });
    }

    public String get(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public Collection<String> hvals(String key) {
        return redisTemplate.<String, String>opsForHash().values(key);
    }

    public Boolean hsetnx(String key, String field, String value) {
        return redisTemplate.opsForHash().putIfAbsent(key, field, value);
    }

    public Long decr(String key) {
        return redisTemplate.opsForValue().increment(key, -1L);

    }

    public Long decrby(String key, Long value) {
        return redisTemplate.opsForValue().increment(key, -value);
    }

    public Long incr(String key) {
        return redisTemplate.opsForValue().increment(key, 1L);
    }

    public Long incrby(String key, Long value) {
        return redisTemplate.opsForValue().increment(key, value);
    }

    public String getrange(String key, Long start, Long end) {
        return redisTemplate.opsForValue().get(key, start, end);
    }

    public Long strlen(String key) {
        return redisTemplate.opsForValue().size(key);
    }

    public List<String> mget(Collection<String> fields) {
        return redisTemplate.opsForValue().multiGet(fields);
    }

    public void mset(Map<String, String> map) {
        redisTemplate.opsForValue().multiSet(map);
    }

    public void msetnx(Map<String, String> map) {
        redisTemplate.opsForValue().multiSetIfAbsent(map);
    }

    public String getset(String key, String value) {
        return redisTemplate.opsForValue().getAndSet(key, value);
    }

    public Boolean setnx(String key, String value) {
        return redisTemplate.opsForValue().setIfAbsent(key, value);
    }

    public void setex(String key, String value, Long timeout, TimeUnit timeUnit) {
        redisTemplate.opsForValue().set(key, value, timeout, timeUnit);
    }

    public void setex(String key, String value, Long offset) {
        redisTemplate.opsForValue().set(key, value, offset);
    }

    public void setbit(final String key, final Long offset, final Boolean value) {
        redisTemplate.execute(new RedisCallback<Object>() {
            @Override
            public Object doInRedis(RedisConnection connection) throws DataAccessException {
                connection.setBit(key.getBytes(), offset, value);
                return null;
            }
        });
    }

    public Boolean getbit(final String key, final Long offset) {
        return redisTemplate.execute(new RedisCallback<Boolean>() {
            @Override
            public Boolean doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.getBit(key.getBytes(), offset);
            }
        });
    }

    public Integer append(String key, String value) {
        return redisTemplate.opsForValue().append(key, value);
    }

    public void multi() {
        redisTemplate.multi();
    }

    public void unwatch() {
        redisTemplate.unwatch();
    }

    public void discard() {
        redisTemplate.discard();
    }

    public void exec() {
        redisTemplate.exec();
    }

    public void watch(Collection<String> keys) {
        redisTemplate.watch(keys);
    }

    public Boolean sadd(String key, String value) {
        return redisTemplate.opsForSet().add(key, value);

    }

    public Long scard(String key) {
        return redisTemplate.opsForSet().size(key);

    }

    public Set<String> sdiff(String key, Collection<String> keys) {
        return redisTemplate.opsForSet().difference(key, keys);
    }

    public void sdiffstore(String key, Collection<String> keys, String destinations) {
        redisTemplate.opsForSet().differenceAndStore(key, keys, destinations);

    }

    public Set<String> sinter(String key, Collection<String> keys) {
        return redisTemplate.opsForSet().intersect(key, keys);
    }

    public void sinterstore(String key, Collection<String> keys, String destination) {
        redisTemplate.opsForSet().intersectAndStore(key, keys, destination);
    }

    public Boolean sismember(String key, String value) {
        return redisTemplate.opsForSet().isMember(key, value);
    }

    public Set<String> smembers(String key) {
        return redisTemplate.opsForSet().members(key);
    }

    public Boolean smove(String key, String value, String destination) {
        return redisTemplate.opsForSet().move(key, value, destination);
    }

    public String spop(String key) {
        return redisTemplate.opsForSet().pop(key);

    }

    public String srandmember(String key) {
        return redisTemplate.opsForSet().randomMember(key);
    }

    public Boolean srem(String key, String value) {
        return redisTemplate.opsForSet().remove(key, value);
    }

    public Set<String> sunion(String key, Collection<String> keys) {
        return redisTemplate.opsForSet().union(key, keys);
    }

    public void sunionstore(String key, Collection<String> keys, String destination) {
        redisTemplate.opsForSet().unionAndStore(key, keys, destination);
    }

    public String echo(final String value) {
        return redisTemplate.execute(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection connection) throws DataAccessException {
                return new String(connection.echo(value.getBytes()));
            }
        });
    }

    public String ping() {
        return redisTemplate.execute(new RedisCallback<String>() {
            @Override
            public String doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.ping();
            }
        });
    }

    public void publish(String channel, Object message) {
        redisTemplate.convertAndSend(channel, message);
    }

    public Object lpop(String key) {
        return redisTemplate.opsForList().leftPop(key);
    }

    public Object blpop(String key, Long timeout) {
        return redisTemplate.opsForList().leftPop(key, timeout, TimeUnit.SECONDS);
    }


    public Object brpoplpush(String key, String destination, Long timeout) {
        return redisTemplate.opsForList().rightPopAndLeftPush(key, destination, timeout, TimeUnit.SECONDS);
    }

    public Object rpoplpush(String key, String destination) {
        return redisTemplate.opsForList().rightPopAndLeftPush(key, destination);
    }

    public String lindex(String key, Long index) {
        return redisTemplate.opsForList().index(key, index);
    }

    public Long linsert(String key, String value, String pivot, String position) {
        if ("BEFORE".equals(position)) {
            return redisTemplate.opsForList().leftPush(key, pivot, value);
        } else if ("AFTER".equals(position)) {
            return redisTemplate.opsForList().rightPush(key, pivot, value);
        } else {
            throw new IllegalArgumentException("Wrong position: " + position);
        }
    }

    public String rpop(String key) {
        return redisTemplate.opsForList().rightPop(key);
    }

    public String brpop(String key, Long timeout) {
        return redisTemplate.opsForList().rightPop(key, timeout, TimeUnit.SECONDS);

    }

    public Long llen(String key) {
        return redisTemplate.opsForList().size(key);
    }

    public List<String> lrange(String key, Long start, Long end) {
        return redisTemplate.opsForList().range(key, start, end);

    }

    public Long lrem(String key, String value, Long count) {
        return redisTemplate.opsForList().remove(key, count, value);
    }

    public void lset(String key, String value, Long index) {
        redisTemplate.opsForList().set(key, index, value);
    }

    public void ltrim(String key, Long start, Long end) {
        redisTemplate.opsForList().trim(key, start, end);
    }


    public Long rpush(String key, String value) {
        return redisTemplate.opsForList().rightPush(key, value);
    }

    public Long rpushx(String key, String value) {
        return redisTemplate.opsForList().rightPushIfPresent(key, value);
    }

    public Long lpush(String key, String value) {
        return redisTemplate.opsForList().leftPush(key, value);
    }

    public void del(Collection<String> keys) {
        redisTemplate.delete(keys);
    }

    public Boolean exists(String key) {
        return redisTemplate.hasKey(key);
    }

    public Boolean expire(String key, Long timeout) {
        return redisTemplate.expire(key, timeout, TimeUnit.SECONDS);
    }

    public Boolean expireat(String key, Long seconds) {
        return redisTemplate.expireAt(key, new Date(seconds * 1000L));
    }

    public Collection<String> keys(String pattern) {
        return redisTemplate.keys(pattern);
    }

    public Boolean move(String key, Integer db) {
        return redisTemplate.move(key, db);
    }

    public Boolean persist(String key) {
        return redisTemplate.persist(key);
    }

    public Boolean pexpire(String key, Long timeout) {
        return redisTemplate.expire(key, timeout, TimeUnit.MILLISECONDS);
    }

    public Boolean pexpireat(String key, Long millis) {
        return redisTemplate.expireAt(key, new Date(millis));
    }

    public String randomkey() {
        return redisTemplate.randomKey();
    }

    public void rename(String key, String value) {
        redisTemplate.rename(key, value);
    }

    public Boolean renamenx(String key, String value) {
        return redisTemplate.renameIfAbsent(key, value);
    }

    public Long ttl(String key) {
        return redisTemplate.getExpire(key);
    }

    public DataType type(String key) {
        return redisTemplate.type(key);
    }

    public List<String> sort(String key) {
        SortQuery<String> sortQuery = SortQueryBuilder.sort(key).build();
        return redisTemplate.sort(sortQuery);
    }

    public Boolean zadd(String key, String value, Double score) {
        return redisTemplate.opsForZSet().add(key, value, score);
    }

    public Long zcard(String key) {
        return redisTemplate.opsForZSet().size(key);
    }

    public Long zcount(String key, Double min, Double max) {
        return redisTemplate.opsForZSet().count(key, min, max);
    }

    public Double zincrby(String key, String value, Double increment) {
        return redisTemplate.opsForZSet().incrementScore(key, value, increment);
    }

    public void zinterstore(String key, Collection<String> keys, String destination) {
        redisTemplate.opsForZSet().intersectAndStore(key, keys, destination);
    }

    public Object zrange(String key, Long start, Long end, Boolean withScore) {
        if (withScore != null && withScore) {
            return redisTemplate.opsForZSet().rangeWithScores(key, start, end);
        }
        return redisTemplate.opsForZSet().range(key, start, end);
    }

    public Set<String> zrangebyscore(String key, Double min, Double max) {
        return redisTemplate.opsForZSet().rangeByScore(key, min, max);
    }

    public Long zrank(String key, String value) {
        return redisTemplate.opsForZSet().rank(key, value);
    }

    public Boolean zrem(String key, String value) {
        return redisTemplate.opsForZSet().remove(key, value);
    }

    public void zremrangebyrank(String key, Long start, Long end) {
        redisTemplate.opsForZSet().removeRange(key, start, end);
    }

    public void zremrangebyscore(String key, Long start, Long end) {
        redisTemplate.opsForZSet().removeRangeByScore(key, start, end);
    }

    public Object zrevrange(String key, Long start, Long end, Boolean withScore) {
        if (withScore != null && withScore) {
            return redisTemplate.opsForZSet().reverseRangeWithScores(key, start, end);
        }

        return redisTemplate.opsForZSet().reverseRange(key, start, end);
    }

    public Set<String> zrevrangebyscore(String key, Double min, Double max) {
        return redisTemplate.opsForZSet().reverseRangeByScore(key, min, max);
    }

    public Long zrevrank(String key, String value) {
        return redisTemplate.opsForZSet().reverseRank(key, value);
    }


    public void zunionstore(String key, Collection<String> keys, String destination) {
        redisTemplate.opsForZSet().unionAndStore(key, keys, destination);
    }
}
