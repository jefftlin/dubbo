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
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RocketMQProtocolTest {

    private static RocketMQProtocolServer rocketMQProtocolServer;
    private static RocketMQProtocol rocketMQProtocol;
    private static String ROCKETMQ_URL_TEMPLATE = "rocketmq://127.0.0.1:9876";
    private static URL registryUrl;


    @BeforeEach
    public void setUp() {
        // init
        rocketMQProtocolServer = new RocketMQProtocolServer();
        rocketMQProtocol = new RocketMQProtocol();
    }

    @AfterEach
    public void tearDown() {
        // release
        rocketMQProtocolServer.close();
        rocketMQProtocol.destroy();
    }

    @Test
    public void testExport() {
        //set params
        Map<String, String> paramMap = new HashMap<>();
        paramMap.put("brokerName", "broker-a");
        paramMap.put("topic", RocketMQProtocolConstant.DUBBO_DEFAULT_PROTOCOL_TOPIC);
        paramMap.put("groupModel", "select");
        paramMap.put("group", "DEFAULT_GROUP");
        paramMap.put("version", "4.9.2");
        paramMap.put("queueId", "");
        paramMap.put("corethreads", "1");
        paramMap.put("threads", "1");

        registryUrl = URL.valueOf(ROCKETMQ_URL_TEMPLATE + "?" + TestRocketMQUtils.mapToString(paramMap));
        Invoker invoker = new RocketMQInvoker<>(RocketMQInvoker.class, registryUrl, this.rocketMQProtocolServer);
        Exporter export = rocketMQProtocol.export(invoker);

        //assert
        Assertions.assertNotNull(export);
    }



}
