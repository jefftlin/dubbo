/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.rpc.protocol.rocketmq;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RocketMQProtocolServerTest {

    private static RocketMQProtocol INSTANCE;
    private static RocketMQProtocolServer rocketMQProtocolServer;
    private static String ROCKETMQ_URL_TEMPLATE = "rocketmq://127.0.0.1:9876";


    @BeforeEach
    public void setUp() {
        // init
        INSTANCE = RocketMQProtocol.getRocketMQProtocol();
        rocketMQProtocolServer = new RocketMQProtocolServer();
    }

    @AfterEach
    public void tearDown() {
        // release
        INSTANCE = null;
        rocketMQProtocolServer.close();
    }

    @Test
    public void testRocketmqProtocolServer() {
        //set params
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("namespace", "");
        paramMap.put("customizedTraceTopic", "topic-dubbo-rpc");
        paramMap.put("corethreads", "1");
        paramMap.put("threads", "1");

        // set serverUrl
        URL serverUrl = URL.valueOf(ROCKETMQ_URL_TEMPLATE + "?" + TestRocketMQUtils.mapToString(paramMap));
        rocketMQProtocolServer.reset(serverUrl);
        MessageListenerConcurrently messageListenerConcurrently = INSTANCE.getMessageListenerConcurrently();
        ((RocketMQProtocol.DubboMessageListenerConcurrently) messageListenerConcurrently).setAttribute("url", serverUrl);
        ((RocketMQProtocol.DubboMessageListenerConcurrently) messageListenerConcurrently).setAttribute("remoteAddress",
            new InetSocketAddress(serverUrl.getHost(),serverUrl.getPort()));
        ((RocketMQProtocol.DubboMessageListenerConcurrently) messageListenerConcurrently).setAttribute("localAddress",
            new InetSocketAddress("127.0.0.1", NetUtils.getAvailablePort()));
        rocketMQProtocolServer.setMessageListenerConcurrently(messageListenerConcurrently);
        try {
            rocketMQProtocolServer.createConsumer();
            //assert
            Assertions.assertNotNull(rocketMQProtocolServer.getDefaultMQProducer());
            Assertions.assertNotNull(rocketMQProtocolServer.getDefaultMQPushConsumer());
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

}
