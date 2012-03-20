package org.apache.camel.component.redis;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.Message;

public class CommandDispatcher {
    private final RedisConfiguration configuration;
    private final Exchange exchange;
    private Object count;

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
            setResult(redisClient.rpush(getKey(), getValue()));
            break;
        case LPUSH:
            setResult(redisClient.lpush(getKey(), getValue()));
            break;
        case LLEN:
            setResult(redisClient.llen(getKey()));
            break;
        case LRANGE:
            setResult(redisClient.lrange(getKey(), getStart(), getEnd()));
            break;
        case LTRIM:
            redisClient.ltrim(getKey(), getStart(), getEnd());
            break;
        case LINDEX:
            setResult(redisClient.lindex(getKey(), getIndex()));
            break;
        case LSET:
            redisClient.lset(getKey(), getValue(), getIndex());
            break;
        case LREM:
            setResult(redisClient.lrem(getKey(), getValue(), getCount()));
            break;
        case LPOP:
            setResult(redisClient.lpop(getKey()));
            break;
        case RPOP:
            setResult(redisClient.rpop(getKey()));
            break;
        case RPOPLPUSH:
            setResult(redisClient.rpoplpush(getKey(), getDestination()));
            break;
        case SADD:
            setResult(redisClient.sadd(getKey(), getValue()));
            break;
        case SMEMBERS:
            setResult(redisClient.smembers(getKey()));
            break;
        case SREM:
            setResult(redisClient.srem(getKey(), getValue()));
            break;
        case SPOP:
            setResult(redisClient.spop(getKey()));
            break;
        case SMOVE:
            setResult(redisClient.smove(getKey(), getValue(), getDestination()));
            break;
        case SCARD:
            setResult(redisClient.scard(getKey()));
            break;
        case SISMEMBER:
            setResult(redisClient.sismember(getKey(), getValue()));
            break;
        case SINTER:
            setResult(redisClient.sinter(getKey(), getKeys()));
            break;
        case SINTERSTORE:
            redisClient.sinterstore(getKey(), getKeys(), getDestination());
            break;
        case SUNION:
            setResult(redisClient.sunion(getKey(), getKeys()));
            break;
        case SUNIONSTORE:
            redisClient.sunionstore(getKey(), getKeys(), getDestination());
            break;
        case SDIFF:
            setResult(redisClient.sdiff(getKey(), getKeys()));
            break;
        case SDIFFSTORE:
            redisClient.sdiffstore(getKey(), getKeys(), getDestination());
            break;
        case SRANDMEMBER:
            setResult(redisClient.srandmember(getKey()));
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
            setResult(redisClient.blpop(getKey(), getTimeout()));
            break;
        case BRPOP:
            setResult(redisClient.brpop(getKey(), getTimeout()));
            break;
        case AUTH:
            notImplemented();
            break;
        case SUBSCRIBE:
            notImplemented();
            break;
        case PUBLISH:
            redisClient.publish(getChannel(), getMessage());
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
            setResult(redisClient.rpushx(getKey(), getValue()));
            break;
        case ECHO:
            setResult(redisClient.echo(getValue()));
            break;
        case LINSERT:
            setResult(redisClient.linsert(getKey(), getValue(), getPivot(), getPosition()));
            break;
        case DEBUG:
            notImplemented();
            break;
        case BRPOPLPUSH:
            setResult(redisClient.brpoplpush(getKey(), getDestination(), getTimeout()));
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

    public String getDestination() {
        return getInHeaderValue(exchange, RedisConstants.DESTINATION, String.class);
    }

    private String getChannel() {
        return getInHeaderValue(exchange, RedisConstants.CHANNEL, String.class);
    }

    private Object getMessage() {
        return getInHeaderValue(exchange, RedisConstants.MESSAGE, Object.class);
    }

    public Long getIndex() {
        return getInHeaderValue(exchange, RedisConstants.INDEX, Long.class);
    }

    public String getPivot() {
        return getInHeaderValue(exchange, RedisConstants.PIVOT, String.class);
    }

    public String getPosition() {
        return getInHeaderValue(exchange, RedisConstants.POSITION, String.class);
    }

    public Long getCount() {
        return getInHeaderValue(exchange, RedisConstants.COUNT, Long.class);
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
}
