package org.apache.camel.component.redis;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

@Ignore
public class RedisConsumerIntegrationTest extends RedisTestSupport {
    private static final JedisConnectionFactory CONNECTION_FACTORY = new JedisConnectionFactory();
    private static final RedisMessageListenerContainer LISTENER_CONTAINER
            = new RedisMessageListenerContainer();

    static {
        CONNECTION_FACTORY.afterPropertiesSet();
        LISTENER_CONTAINER.setConnectionFactory(CONNECTION_FACTORY);
        LISTENER_CONTAINER.afterPropertiesSet();
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();

        redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(CONNECTION_FACTORY);
        redisTemplate.afterPropertiesSet();

        registry.bind("redisTemplate", redisTemplate);
        registry.bind("listenerContainer", LISTENER_CONTAINER);
        return registry;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from("redis://localhost:6379?command=SUBSCRIBE&channels=one,two&listenerContainer=#listenerContainer&redisTemplate=#redisTemplate")
                        .to("mock:result");

                from("direct:start")
                        .to("redis://localhost:6379?redisTemplate=#redisTemplate");
            }
        };
    }

    @Test
    public void consumerReceivesMessages() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedMinimumMessageCount(1);
        mock.expectedBodiesReceived("message");

        sendHeaders(
                       RedisConstants.COMMAND, "PUBLISH",
                       RedisConstants.CHANNEL, "two",
                       RedisConstants.MESSAGE, "message");
        mock.assertIsSatisfied();
    }
}

