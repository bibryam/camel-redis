package org.apache.camel.component.redis;

import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

public class RedisConfiguration {
    private String command;
    private String channels;
    private Integer timeout;
    private String host;
    private Integer port;
    private RedisTemplate<String, String> redisTemplate;
    private RedisMessageListenerContainer listenerContainer;

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getTimeout() {
        return timeout;
    }

    public void setTimeout(Integer timeout) {
        this.timeout = timeout;
    }

    public RedisTemplate<String, String> getRedisTemplate() {
        return redisTemplate != null ? redisTemplate : createDefaultTemplate();
    }

    public void setRedisTemplate(RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    public RedisMessageListenerContainer getListenerContainer() {
        return listenerContainer != null ? listenerContainer : createDefaultListenerContainer();
    }

    public void setListenerContainer(RedisMessageListenerContainer listenerContainer) {
        this.listenerContainer = listenerContainer;
    }

    public String getChannels() {
        return channels;
    }

    public void setChannels(String channels) {
        this.channels = channels;
    }

    private RedisTemplate<String, String> createDefaultTemplate() {
        JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
        if (host != null) {
            connectionFactory.setHostName(host);
        }
        if (port != null) {
            connectionFactory.setPort(port);
        }
        connectionFactory.afterPropertiesSet();
        redisTemplate = new RedisTemplate();
        redisTemplate.setConnectionFactory(connectionFactory);
        redisTemplate.afterPropertiesSet();
        return redisTemplate;
    }

    private RedisMessageListenerContainer createDefaultListenerContainer() {
        JedisConnectionFactory connectionFactory = new JedisConnectionFactory();
        if (host != null) {
            connectionFactory.setHostName(host);
        }
        if (port != null) {
            connectionFactory.setPort(port);
        }
        connectionFactory.afterPropertiesSet();

        listenerContainer = new RedisMessageListenerContainer();

        listenerContainer.setConnectionFactory(connectionFactory);
        listenerContainer.afterPropertiesSet();
        return listenerContainer;
    }

}
