package org.apache.camel.component.redis;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

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
}
