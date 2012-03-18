package org.apache.camel.component.redis;

import org.apache.camel.impl.JndiRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RedisConnectionTest extends RedisTestSupport {

    private RedisTemplate redisTemplate;
    private RedisConnection redisConnection;

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();
        registry.bind("redisTemplate", redisTemplate);
        return registry;
    }

    @Before
    public void setUp() throws Exception {
        redisTemplate = mock(RedisTemplate.class);
        redisConnection = mock(RedisConnection.class);
        super.setUp();
    }

    private void getMocketConnection() {
        ArgumentCaptor<RedisCallback> argument = ArgumentCaptor.forClass(RedisCallback.class);
        verify(redisTemplate).execute(argument.capture());
        RedisCallback redisCallback = argument.getValue();
        redisCallback.doInRedis(redisConnection);
    }

    @Test
    public void shouldExecuteECHO() throws Exception {
        when(redisTemplate.execute(any(RedisCallback.class))).thenReturn("value");

        Object result = sendHeaders(
                RedisConstants.COMMAND, "ECHO",
                RedisConstants.VALUE, "value");

        assertEquals("value", result);
    }

    @Test
    public void shouldExecutePING() throws Exception {
        when(redisTemplate.execute(any(RedisCallback.class))).thenReturn("PONG");

        Object result = sendHeaders(RedisConstants.COMMAND, "PING");

        assertEquals("PONG", result);
    }

    @Test
    public void shouldExecuteQUIT() throws Exception {
        sendHeaders(RedisConstants.COMMAND, "QUIT");

        verify(redisTemplate).execute(any(RedisCallback.class));
    }


    @Test
    public void shouldExecutePUBLISH() throws Exception {
        Object result = sendHeaders(
                RedisConstants.COMMAND, "PUBLISH",
                RedisConstants.CHANNEL, "channel",
                RedisConstants.MESSAGE, "a message");

        verify(redisTemplate).convertAndSend("channel", "a message");
    }

}
