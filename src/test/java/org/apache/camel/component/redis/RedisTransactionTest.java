package org.apache.camel.component.redis;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.impl.JndiRegistry;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RedisTransactionTest extends RedisTestSupport {
    private RedisTemplate redisTemplate;

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();
        registry.bind("redisTemplate", redisTemplate);
        return registry;
    }

    @Before
    public void setUp() throws Exception {
        redisTemplate = mock(RedisTemplate.class);
        super.setUp();
    }

    @Test
    public void shouldExecuteMULTI() throws Exception {
        sendHeaders(RedisConstants.COMMAND, "MULTI");
        verify(redisTemplate).multi();
    }

    @Test
    public void shouldExecuteDISCARD() throws Exception {
        sendHeaders(RedisConstants.COMMAND, "DISCARD");
        verify(redisTemplate).discard();
    }

    @Test
    public void shouldExecuteEXEC() throws Exception {
        sendHeaders(RedisConstants.COMMAND, "EXEC");
        verify(redisTemplate).exec();
    }

    @Test
    public void shouldExecuteUNWATCH() throws Exception {
        sendHeaders(RedisConstants.COMMAND, "UNWATCH");
        verify(redisTemplate).unwatch();
    }

    @Test
    public void shouldExecuteWATCH() throws Exception {
        List<String> keys = new ArrayList<String>();
        keys.add("key");

        sendHeaders(
                RedisConstants.COMMAND, "WATCH",
                RedisConstants.KEYS, keys);
        verify(redisTemplate).watch(keys);
    }

}

