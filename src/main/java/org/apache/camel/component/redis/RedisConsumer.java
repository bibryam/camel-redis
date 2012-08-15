package org.apache.camel.component.redis;

import java.io.UnsupportedEncodingException;
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
        try {
            Exchange exchange = getEndpoint().createExchange();
            setChannel(exchange, message.getChannel());
            setPattern(exchange, pattern);
            setBody(exchange, message.getBody());
            getProcessor().process(exchange);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setBody(Exchange exchange, byte[] body) {
        if (body != null) {
            exchange.getIn().setBody(redisConfiguration.getSerializer().deserialize(body));
        }
    }

    private void setPattern(Exchange exchange, byte[] pattern) {
        if (pattern != null) {
            exchange.getIn().setHeader(RedisConstants.PATTERN, pattern);
        }
    }

    private void setChannel(Exchange exchange, byte[] message) throws UnsupportedEncodingException {
        if (message != null) {
            exchange.getIn().setHeader(RedisConstants.CHANNEL, new String(message, "UTF8"));
        }
    }
}
