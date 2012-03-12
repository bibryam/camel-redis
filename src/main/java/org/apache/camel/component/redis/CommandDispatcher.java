package org.apache.camel.component.redis;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Message;

public class CommandDispatcher {
    private final RedisConfiguration configuration;
    private final Exchange exchange;

    public CommandDispatcher(RedisConfiguration configuration, Exchange exchange) {
        this.configuration = configuration;
        this.exchange = exchange;
    }

    public void execute(final RedisClient redisClient) {
        switch (determineCommand()) {

        case PING:
            setResult(redisClient.ping());
            break;
        case SET:
            redisClient.set(getKey(), getValue());
            break;
        case GET:
            setResult(redisClient.get(getKey()));
            break;
        case QUIT:
            redisClient.quit();
            break;
        case EXISTS:
            notImplemented();
            break;
        case DEL:
            notImplemented();
            break;
        case TYPE:
            notImplemented();
            break;
        case FLUSHDB:
            notImplemented();
            break;
        case KEYS:
            notImplemented();
            break;
        case RANDOMKEY:
            notImplemented();
            break;
        case RENAME:
            notImplemented();
            break;
        case RENAMENX:
            notImplemented();
            break;
        case RENAMEX:
            notImplemented();
            break;
        case DBSIZE:
            notImplemented();
            break;
        case EXPIRE:
            notImplemented();
            break;
        case EXPIREAT:
            notImplemented();
            break;
        case TTL:
            notImplemented();
            break;
        case SELECT:
            notImplemented();
            break;
        case MOVE:
            notImplemented();
            break;
        case FLUSHALL:
            notImplemented();
            break;
        case GETSET:
            setResult(redisClient.getset(getKey(), getValue()));
            break;
        case MGET:
            setResult(redisClient.mget(getFields()));
            break;
        case SETNX:
            setResult(redisClient.setnx(getKey(), getValue()));
            break;
        case SETEX:
            redisClient.setex(getKey(), getValue(), getTimeout(), TimeUnit.SECONDS);
            break;
        case MSET:
            redisClient.mset(getValuesAsMap());
            break;
        case MSETNX:
            redisClient.msetnx(getValuesAsMap());
            break;
        case DECRBY:
            setResult(redisClient.decrby(getKey(), Long.valueOf(getValue())));
            break;
        case DECR:
            setResult(redisClient.decr(getKey()));
            break;
        case INCRBY:
            setResult(redisClient.incrby(getKey(), Long.valueOf(getValue())));
            break;
        case INCR:
            setResult(redisClient.incr(getKey()));
            break;
        case APPEND:
            setResult(redisClient.append(getKey(), getValue()));
            break;
        case SUBSTR:
            notImplemented();
            break;
        case HSET:
            redisClient.hset(getKey(), getField(), getValue());
            break;
        case HGET:
            setResult(redisClient.hget(getKey(), getField()));
            break;
        case HSETNX:
            setResult(redisClient.hsetnx(getKey(), getField(), getValue()));
            break;
        case HMSET:
            redisClient.hmset(getKey(), getValuesAsMap());
            break;
        case HMGET:
            setResult(redisClient.hmget(getKey(), getFields()));
            break;
        case HINCRBY:
            setResult(redisClient.hincrBy(getKey(), getField(), getValueAsLong()));
            break;
        case HEXISTS:
            setResult(redisClient.hexists(getKey(), getField()));
            break;
        case HDEL:
            redisClient.hdel(getKey(), getField());
            break;
        case HLEN:
            setResult(redisClient.hlen(getKey()));
            break;
        case HKEYS:
            setResult(redisClient.hkeys(getKey()));
            break;
        case HVALS:
            setResult(redisClient.hvals(getKey()));
            break;
        case HGETALL:
            setResult(redisClient.hgetAll(getKey()));
            break;
        case RPUSH:
            notImplemented();
            break;
        case LPUSH:
            notImplemented();
            break;
        case LLEN:
            notImplemented();
            break;
        case LRANGE:
            notImplemented();
            break;
        case LTRIM:
            notImplemented();
            break;
        case LINDEX:
            notImplemented();
            break;
        case LSET:
            notImplemented();
            break;
        case LREM:
            notImplemented();
            break;
        case LPOP:
            notImplemented();
            break;
        case RPOP:
            notImplemented();
            break;
        case RPOPLPUSH:
            notImplemented();
            break;
        case SADD:
            notImplemented();
            break;
        case SMEMBERS:
            notImplemented();
            break;
        case SREM:
            notImplemented();
            break;
        case SPOP:
            notImplemented();
            break;
        case SMOVE:
            notImplemented();
            break;
        case SCARD:
            notImplemented();
            break;
        case SISMEMBER:
            notImplemented();
            break;
        case SINTER:
            notImplemented();
            break;
        case SINTERSTORE:
            notImplemented();
            break;
        case SUNION:
            notImplemented();
            break;
        case SUNIONSTORE:
            notImplemented();
            break;
        case SDIFF:
            notImplemented();
            break;
        case SDIFFSTORE:
            notImplemented();
            break;
        case SRANDMEMBER:
            notImplemented();
            break;
        case ZADD:
            notImplemented();
            break;
        case ZRANGE:
            notImplemented();
            break;
        case ZREM:
            notImplemented();
            break;
        case ZINCRBY:
            notImplemented();
            break;
        case ZRANK:
            notImplemented();
            break;
        case ZREVRANK:
            notImplemented();
            break;
        case ZREVRANGE:
            notImplemented();
            break;
        case ZCARD:
            notImplemented();
            break;
        case ZSCORE:
            notImplemented();
            break;
        case MULTI:
            redisClient.multi();
            break;
        case DISCARD:
            redisClient.discard();
            break;
        case EXEC:
            redisClient.exec();
            break;
        case WATCH:
            redisClient.watch(getKeys());
            break;
        case UNWATCH:
            redisClient.unwatch();
            break;
        case SORT:
            notImplemented();
            break;
        case BLPOP:
            notImplemented();
            break;
        case BRPOP:
            notImplemented();
            break;
        case AUTH:
            notImplemented();
            break;
        case SUBSCRIBE:
            notImplemented();
            break;
        case PUBLISH:
            notImplemented();
            break;
        case UNSUBSCRIBE:
            notImplemented();
            break;
        case PSUBSCRIBE:
            notImplemented();
            break;
        case PUNSUBSCRIBE:
            notImplemented();
            break;
        case ZCOUNT:
            notImplemented();
            break;
        case ZRANGEBYSCORE:
            notImplemented();
            break;
        case ZREVRANGEBYSCORE:
            notImplemented();
            break;
        case ZREMRANGEBYRANK:
            notImplemented();
            break;
        case ZREMRANGEBYSCORE:
            notImplemented();
            break;
        case ZUNIONSTORE:
            notImplemented();
            break;
        case ZINTERSTORE:
            notImplemented();
            break;
        case SAVE:
            notImplemented();
            break;
        case BGSAVE:
            notImplemented();
            break;
        case BGREWRITEAOF:
            notImplemented();
            break;
        case LASTSAVE:
            notImplemented();
            break;
        case SHUTDOWN:
            notImplemented();
            break;
        case INFO:
            notImplemented();
            break;
        case MONITOR:
            notImplemented();
            break;
        case SLAVEOF:
            notImplemented();
            break;
        case CONFIG:
            notImplemented();
            break;
        case STRLEN:
            setResult(redisClient.strlen(getKey()));
            break;
        case SYNC:
            notImplemented();
            break;
        case LPUSHX:
            notImplemented();
            break;
        case PERSIST:
            notImplemented();
            break;
        case RPUSHX:
            notImplemented();
            break;
        case ECHO:
            notImplemented();
            break;
        case LINSERT:
            notImplemented();
            break;
        case DEBUG:
            notImplemented();
            break;
        case BRPOPLPUSH:
            notImplemented();
            break;
        case SETBIT:
            redisClient.setbit(getKey(), getOffset(), Boolean.valueOf(getValue()));
            break;
        case GETBIT:
            setResult(redisClient.getbit(getKey(), getOffset()));
            break;
        case SETRANGE:
            redisClient.setex(getKey(), getValue(), getOffset());
            break;
        case GETRANGE:
            setResult(redisClient.getrange(getKey(), getStart(), getEnd()));
            break;
        default:
            throw new IllegalArgumentException("Unsupported command");
        }
    }

    private Long getStart() {
        return getInHeaderValue(exchange, RedisConstants.START, Long.class);
    }

    private Long getEnd() {
        return getInHeaderValue(exchange, RedisConstants.END, Long.class);
    }


    private Long getTimeout() {
        return getInHeaderValue(exchange, RedisConstants.TIMEOUT, Long.class);
    }

    private Long getOffset() {
        return getInHeaderValue(exchange, RedisConstants.OFFSET, Long.class);
    }

    private Long getValueAsLong() {
        return getInHeaderValue(exchange, RedisConstants.VALUE, Long.class);
    }

    private Collection<String> getFields() {
        return getInHeaderValue(exchange, RedisConstants.FIELDS, Collection.class);
    }

    private HashMap getValuesAsMap() {
        return getInHeaderValue(exchange, RedisConstants.VALUES, new HashMap<String, String>().getClass());
    }

    private String getKey() {
        return getInHeaderValue(exchange, RedisConstants.KEY, String.class);
    }


    public Collection<String> getKeys() {
        return getInHeaderValue(exchange, RedisConstants.KEYS, Collection.class);
    }

    private String getValue() {
        return getInHeaderValue(exchange, RedisConstants.VALUE, String.class);
    }

    private String getField() {
        return getInHeaderValue(exchange, RedisConstants.FIELD, String.class);
    }

    private void notImplemented() {
        setResult("Not implemented");
    }

    private Command determineCommand() {
        String command = exchange.getIn().getHeader(RedisConstants.COMMAND, String.class);
        if (command == null) {
            command = configuration.getCommand();
        }
        if (command == null) {
            return Command.SET;
        }
        return Command.valueOf(command);
    }

    private static <T> T getInHeaderValue(Exchange exchange, String key, Class<T> aClass) {
        return exchange.getIn().getHeader(key, aClass);
    }

    private void setResult(Object result) {
        Message message;
        if (exchange.getPattern().isOutCapable()) {
            message = exchange.getOut();
            message.copyFrom(exchange.getIn());
        } else {
            message = exchange.getIn();
        }
        message.setBody(result);
    }

}
