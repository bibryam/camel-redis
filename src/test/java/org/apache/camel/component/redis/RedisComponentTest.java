package org.apache.camel.component.redis;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.camel.impl.JndiRegistry;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RedisComponentTest extends RedisTestSupport {

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();
        jedis = mock(Jedis.class);
        registry.bind("jedis", jedis);
        return registry;
    }

    @Test
    public void shouldExecuteSET() throws Exception {
        when(jedis.set(anyString(), anyString())).thenReturn("OK");

        Object result = sendHeaders(
                RedisConstants.KEY, "key",
                RedisConstants.VALUE, "value");

        verify(jedis).set("key", "value");
        assertEquals("OK", result);
    }

    @Test
    public void shouldExecuteGET() throws Exception {
        when(jedis.get("key")).thenReturn("value");

        Object result = sendHeaders(
                RedisConstants.KEY, "key",
                RedisConstants.COMMAND, "GET");

        verify(jedis).get("key");
        assertEquals("value", result);
    }

    @Test
    public void shouldExecuteHDEL() throws Exception {
        when(jedis.hdel(anyString(), anyString())).thenReturn(1L);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HDEL",
                RedisConstants.KEY, "key",
                RedisConstants.FIELD, "field");

        verify(jedis).hdel("key", "field");
        assertEquals(1L, result);
    }


    @Test
    public void shouldExecuteHEXISTS() throws Exception {
        when(jedis.hexists(anyString(), anyString())).thenReturn(true);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HEXISTS",
                RedisConstants.KEY, "key",
                RedisConstants.FIELD, "field");

        verify(jedis).hexists("key", "field");
        assertEquals(true, result);
    }


    @Test
    public void shouldExecuteHINCRBY() throws Exception {
        when(jedis.hincrBy(anyString(), anyString(), anyLong())).thenReturn(1L);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HINCRBY",
                RedisConstants.KEY, "key",
                RedisConstants.FIELD, "field",
                RedisConstants.VALUE, "1");

        verify(jedis).hincrBy("key", "field", 1L);
        assertEquals(1L, result);
    }

    @Test
    public void shouldExecuteHKEYS() throws Exception {
        Set<String> fields = new HashSet<String>(Arrays.asList(new String[] {"field1, field2"}));
        when(jedis.hkeys(anyString())).thenReturn(fields);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HKEYS",
                RedisConstants.KEY, "key");

        verify(jedis).hkeys("key");
        assertEquals(fields, result);
    }

    @Test
    public void shouldExecuteHMSET() throws Exception {
        HashMap<String, String> values = new HashMap<String, String>();
        values.put("field1", "value1");
        values.put("field2", "value");
        when(jedis.hmset(anyString(), anyMap())).thenReturn("OK");

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HMSET",
                RedisConstants.KEY, "key",
                RedisConstants.VALUES, values);

        verify(jedis).hmset("key", values);
        assertEquals("OK", result);
    }

    @Test
    public void shouldExecuteHVALS() throws Exception {
        List<String> values = new ArrayList<String>();
        values.add("val1");
        values.add("val2");

        when(jedis.hvals(anyString())).thenReturn(values);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HVALS",
                RedisConstants.KEY, "key",
                RedisConstants.VALUES, values);

        verify(jedis).hvals("key");
        assertEquals(values, result);
    }


    @Test
    public void shouldExecuteHLEN() throws Exception {
        when(jedis.hlen(anyString())).thenReturn(2L);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HLEN",
                RedisConstants.KEY, "key");

        verify(jedis).hlen("key");
        assertEquals(2L, result);
    }


    @Test
    public void shouldSetHashValue() throws Exception {
        when(jedis.hset(anyString(), anyString(), anyString())).thenReturn(1L);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HSET",
                RedisConstants.KEY, "key",
                RedisConstants.FIELD, "field",
                RedisConstants.VALUE, "value");

        verify(jedis).hset("key", "field", "value");
        assertEquals(1L, result);
    }

    @Test
    public void shouldExecuteHSETNX() throws Exception {
        when(jedis.hsetnx(anyString(), anyString(), anyString())).thenReturn(1L);

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HSETNX",
                RedisConstants.KEY, "key",
                RedisConstants.FIELD, "field",
                RedisConstants.VALUE, "value");

        verify(jedis).hsetnx("key", "field", "value");
        assertEquals(1L, result);
    }

    @Test
    public void shouldExecuteHGET() throws Exception {
        when(jedis.hget(anyString(), anyString())).thenReturn("value");

        Object result = sendHeaders(
                RedisConstants.COMMAND, "HGET",
                RedisConstants.KEY, "key",
                RedisConstants.FIELD, "field");

        verify(jedis).hget("key", "field");
        assertEquals("value", result);
    }

    @Test
    public void shouldExecuteMULTI() throws Exception {
        when(jedis.multi()).thenReturn(any(Transaction.class));

        Object result = sendHeaders(RedisConstants.COMMAND, "MULTI");

        verify(jedis).multi();
        assertEquals("OK", result);
    }

}
