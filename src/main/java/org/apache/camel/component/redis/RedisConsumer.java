package org.apache.camel.component.redis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.direct.DirectConsumer;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.PatternTopic;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.serializer.RedisSerializer;

public class RedisConsumer extends DirectConsumer implements MessageListener {
    private final RedisConfiguration redisConfiguration;

    public RedisConsumer(RedisEndpoint redisEndpoint, Processor processor,
                         RedisConfiguration redisConfiguration) {
        super(redisEndpoint, processor);
        this.redisConfiguration = redisConfiguration;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        Collection<Topic> topics = toTopics(redisConfiguration.getChannels());
        redisConfiguration.getListenerContainer().addMessageListener(this, topics);
    }

    private Collection<Topic> toTopics(String channels) {
        String[] channelsArrays = channels.split(",");
        List<Topic> topics = new ArrayList<Topic>();
        for (String channel : channelsArrays) {
            if (Command.PSUBSCRIBE.toString().equals(redisConfiguration.getCommand())) {
                topics.add(new PatternTopic(channel));
            } else if (Command.SUBSCRIBE.toString().equals(redisConfiguration.getCommand())) {
                topics.add(new ChannelTopic(channel));
            } else {
                throw new RuntimeException("Unsupported Command");
            }
        }
        return topics;
    }

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String patternUsed = pattern != null ? new String(pattern) : null;
        String channelFrom = message.getChannel() != null ? new String(message.getChannel()) : null;
        Object value = getMessage(message);

        Exchange exchange = getEndpoint().createExchange();
        exchange.getIn().setHeader(RedisConstants.CHANNEL, channelFrom);
        exchange.getIn().setHeader(RedisConstants.PATTERN, patternUsed);
        exchange.getIn().setBody(value);
        try {
            getProcessor().process(exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object getMessage(Message message) {
        if (redisConfiguration.getRedisTemplate() != null) {
            RedisSerializer defaultSerializer = redisConfiguration.getRedisTemplate().getDefaultSerializer();
            return defaultSerializer.deserialize(message.getBody());
        }
        return message.getBody();
    }
}
