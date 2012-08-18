package org.apache.camel.component.redis.processor.idempotent;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SetOperations;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RedisIdempotentRepositoryTest {
    private static final String REPOSITORY = "testRepository";
    private static final String KEY = "KEY";
    private RedisTemplate redisTemplate;
    private SetOperations setOperations;
    private RedisIdempotentRepository idempotentRepository;

    @Before
    public void setUp() throws Exception {
        redisTemplate = mock(RedisTemplate.class);
        setOperations = mock(SetOperations.class);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        idempotentRepository = new RedisIdempotentRepository(redisTemplate, REPOSITORY);
    }

    @Test
    public void shouldAddKey() {
        idempotentRepository.add(KEY);
        verify(setOperations).add(REPOSITORY, KEY);
    }

    @Test
    public void shoulCheckForMembers() {
        idempotentRepository.contains(KEY);
        verify(setOperations).isMember(REPOSITORY, KEY);
    }

    @Test
    public void shouldRemoveKey() {
        idempotentRepository.remove(KEY);
        verify(setOperations).remove(REPOSITORY, KEY);
    }

    @Test
    public void shouldReturnProcessorName() {
        String processorName = idempotentRepository.getProcessorName();
        assertThat(processorName, is(REPOSITORY));
    }
}
