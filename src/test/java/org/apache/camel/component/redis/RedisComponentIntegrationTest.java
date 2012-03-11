/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.redis;

import org.apache.camel.impl.JndiRegistry;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;

public class RedisComponentIntegrationTest extends RedisTestSupport {

    @BeforeClass
    public static void setupJedis() {
        jedis = new Jedis("localhost", 6379, 500);
        jedis.connect();
        jedis.flushAll();
    }

    @AfterClass
    public static void tearDownJedis() throws Exception {
        jedis.disconnect();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        jedis.flushAll();
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry registry = super.createRegistry();
        registry.bind("jedis", jedis);
        return registry;
    }

    @Test
    public void shouldSetAString() throws Exception {
        Object result = sendHeaders(RedisConstants.KEY, "key1", RedisConstants.VALUE, "value");

        assertEquals("value", jedis.get("key1"));
        assertEquals("OK", result);
    }

    @Test
    public void shouldGetAString() throws Exception {
        jedis.set("key2", "value");
        Object result = sendHeaders(RedisConstants.KEY, "key2", RedisConstants.COMMAND, "GET");

        assertEquals("value", result);
    }

}
