package org.apache.camel.component.redis.processor.idempotent;

import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedOperation;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.component.redis.RedisConfiguration;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.support.ServiceSupport;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

@ManagedResource(description = "Redis based message id repository")
public class RedisIdempotentRepository extends ServiceSupport implements IdempotentRepository<String> {
    private final SetOperations<String, String> setOperations;
    private final String processorName;
    private RedisConfiguration redisConfiguration;

    public RedisIdempotentRepository(RedisTemplate<String, String> redisTemplate, String processorName) {
        this.setOperations = redisTemplate.opsForSet();
        this.processorName = processorName;
    }

    public RedisIdempotentRepository(String processorName) {
        redisConfiguration = new RedisConfiguration();
        RedisTemplate<String, String> redisTemplate = redisConfiguration.getRedisTemplate();
        this.setOperations = redisTemplate.opsForSet();
        this.processorName = processorName;
    }

    public static RedisIdempotentRepository redisIdempotentRepository(String processorName) {
        return new RedisIdempotentRepository(processorName);
    }

    public static RedisIdempotentRepository redisIdempotentRepository(
            RedisTemplate<String, String> redisTemplate, String processorName) {
        return new RedisIdempotentRepository(redisTemplate, processorName);
    }

    @ManagedOperation(description = "Adds the key to the store")
    public boolean add(String key) {
        return setOperations.add(processorName, key);
    }

    @ManagedOperation(description = "Does the store contain the given key")
    public boolean contains(String key) {
        return setOperations.isMember(processorName, key);
    }

    @ManagedOperation(description = "Remove the key from the store")
    public boolean remove(String key) {
        return setOperations.remove(processorName, key);
    }

    @ManagedAttribute(description = "The processor name")
    public String getProcessorName() {
        return processorName;
    }

    public boolean confirm(String key) {
        return true;
    }

    protected void doShutdown() throws Exception {
        super.doShutdown();
        if (redisConfiguration != null) {
            redisConfiguration.stop();
        }
    }

    protected void doStart() throws Exception {
    }

    protected void doStop() throws Exception {
    }
}

