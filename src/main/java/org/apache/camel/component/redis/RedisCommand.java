package org.apache.camel.component.redis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.Message;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Transaction;

public class RedisCommand {
    private final Jedis jedis;
    private final RedisConfiguration configuration;
    private final Exchange exchange;

    public RedisCommand(Jedis jedis, RedisConfiguration configuration, Exchange exchange) {
        this.jedis = jedis;
        this.configuration = configuration;
        this.exchange = exchange;
    }

    public void execute() {
        switch (determineCommand()) {

        case PING:
            executePing();
            break;
        case SET:
            executeSet();
            break;
        case GET:
            executeGet();
            break;
        case QUIT:
            executeQuit();
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
            notImplemented();
            break;
        case MGET:
            notImplemented();
            break;
        case SETNX:
            notImplemented();
            break;
        case SETEX:
            notImplemented();
            break;
        case MSET:
            notImplemented();
            break;
        case MSETNX:
            notImplemented();
            break;
        case DECRBY:
            notImplemented();
            break;
        case DECR:
            notImplemented();
            break;
        case INCRBY:
            notImplemented();
            break;
        case INCR:
            notImplemented();
            break;
        case APPEND:
            notImplemented();
            break;
        case SUBSTR:
            notImplemented();
            break;
        case HSET:
            executeHSET();
            break;
        case HGET:
            executeHGET();
            break;
        case HSETNX:
            executeHSETNX();
            break;
        case HMSET:
            executeHMSET();
            break;
        case HMGET:
            executeHMGET();
            break;
        case HINCRBY:
            exeuteHINCRBY();
            break;
        case HEXISTS:
            executeHEXISTS();
            break;
        case HDEL:
            executeHDEL();
            break;
        case HLEN:
            executeHLEN();
            break;
        case HKEYS:
            executeHKEYS();
            break;
        case HVALS:
            executeHVALS();
            break;
        case HGETALL:
            executeHGETALL();
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
            executeMULTI();
            break;
        case DISCARD:
            notImplemented();
            break;
        case EXEC:
            notImplemented();
            break;
        case WATCH:
            notImplemented();
            break;
        case UNWATCH:
            notImplemented();
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
            notImplemented();
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
            notImplemented();
            break;
        case GETBIT:
            notImplemented();
            break;
        case SETRANGE:
            notImplemented();
            break;
        case GETRANGE:
            notImplemented();
            break;
        default:
            throw new IllegalArgumentException("Unsupported command");
        }
    }

    private void executeMULTI() {

        Transaction multi = jedis.multi();
     }

    private void executeHVALS() {
        List<String> result = jedis.hvals(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class));
        setResult(result);
    }

    private void executeHSETNX() {
        Long result = jedis.hsetnx(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELD, String.class),
                getInHeaderValue(exchange, RedisConstants.VALUE, String.class));
        setResult(result);
    }

    private void executeHMSET() {
        String result = jedis.hmset(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.VALUES, new HashMap<String, String>().getClass()));
        setResult(result);

    }

    private void executeHMGET() {
        List<String> result = jedis.hmget(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELDS, new String[] {}.getClass()));
        setResult(result);
    }

    private void executeHKEYS() {
        Set<String> result = jedis.hkeys(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class));
        setResult(result);
    }

    private void executeHLEN() {
        Long result = jedis.hlen(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class));
        setResult(result);
    }

    private void exeuteHINCRBY() {
        Long result = jedis.hincrBy(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELD, String.class),
                getInHeaderValue(exchange, RedisConstants.VALUE, Long.class));
        setResult(result);
    }

    private void executeHGETALL() {
        Map<String, String> result = jedis
                .hgetAll(getInHeaderValue(exchange, RedisConstants.KEY, String.class));
        setResult(result);
    }

    private void executeHEXISTS() {
        Boolean result = jedis.hexists(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELD, String.class));
        setResult(result);
    }

    private void executeHGET() {
        String result = jedis.hget(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELD, String.class));
        setResult(result);
    }

    private void executeHDEL() {
        Long result = jedis.hdel(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELD, String.class));
        setResult(result);
    }

    private void executeHSET() {
        Long result = jedis.hset(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.FIELD, String.class),
                getInHeaderValue(exchange, RedisConstants.VALUE, String.class));
        setResult(result);
    }

    private void executeQuit() {
        setResult(jedis.quit());
    }

    private void executeGet() {
        setResult(jedis.get(getInHeaderValue(exchange, RedisConstants.KEY, String.class)));
    }

    private void executePing() {
        setResult(jedis.ping());
    }

    private void executeSet() {
        String result = jedis.set(
                getInHeaderValue(exchange, RedisConstants.KEY, String.class),
                getInHeaderValue(exchange, RedisConstants.VALUE, String.class));
        setResult(result);
    }

    private void notImplemented() {
        setResult("Not implemented");
    }

    private Protocol.Command determineCommand() {
        String command = exchange.getIn().getHeader(RedisConstants.COMMAND, String.class);
        if (command == null) {
            command = configuration.getCommand();
        }
        if (command == null) {
            return Protocol.Command.SET;
        }
        return Protocol.Command.valueOf(command);
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
