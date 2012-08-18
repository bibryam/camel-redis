package org.apache.camel.component.redis.processor.idempotent;

import org.apache.camel.api.management.ManagedAttribute;
import org.apache.camel.api.management.ManagedOperation;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.spi.IdempotentRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

@ManagedResource(description = "Redis based message id repository")
public class RedisIdempotentRepository implements IdempotentRepository<String> {
    private final SetOperations<String, String> setOperations;
    private final String processorName;

    public RedisIdempotentRepository(RedisTemplate<String, String> redisTemplate, String processorName) {
        setOperations = redisTemplate.opsForSet();
        this.processorName = processorName;
    }

    public static RedisIdempotentRepository redisIdempotentRepository(RedisTemplate<String, String> redisTemplate,
                                                                      String processorName) {
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
        // noop
        return true;
    }

    public void start() throws Exception {
        // noop
    }

    public void stop() throws Exception {
        // noop
    }
}

