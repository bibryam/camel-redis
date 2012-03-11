package org.apache.camel.component.redis;

import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit4.CamelTestSupport;

import redis.clients.jedis.Jedis;

public class RedisTestSupport extends CamelTestSupport {

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    protected static Jedis jedis;

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from("direct:start")
                        .to("redis://localhost:8080?jedis=#jedis&command=SET")
                        .to("mock:result");
            }
        };
    }

    protected Object sendHeaders(final Object... headers) {
        Exchange exchange = template.send(new Processor() {
            public void process(Exchange exchange) throws Exception {
                Message in = exchange.getIn();
                for (int i = 0; i < headers.length; i = i + 2) {
                    in.setHeader(headers[i].toString(), headers[i + 1]);
                }
            }
        });

        return exchange.getIn().getBody();
    }
}
