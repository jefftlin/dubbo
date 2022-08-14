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

import static org.apache.dubbo.remoting.Constants.DEFAULT_REMOTING_SERIALIZATION;
import static org.apache.dubbo.remoting.Constants.SERIALIZATION_KEY;
import static org.apache.dubbo.rpc.Constants.SERIALIZATION_ID_KEY;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RocketMQProtocolInvokerTest {

    private static RocketMQProtocolServer rocketMQProtocolServer;
    private static RocketMQInvoker rocketMQInvoker;
    private static String ROCKETMQ_URL_TEMPLATE = "rocketmq://127.0.0.1:9876";
    private static URL registryUrl;


    @BeforeEach
    public void setUp() {
        // init
        rocketMQProtocolServer = new RocketMQProtocolServer();

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
        rocketMQProtocolServer.reset(registryUrl);
    }

    @AfterEach
    public void tearDown() {
        // release
        rocketMQInvoker.destroy();
    }

    @Test
    public void testInvoke() {
        // init invoker
        rocketMQInvoker = new RocketMQInvoker<>(RocketMQInvoker.class, registryUrl, this.rocketMQProtocolServer);

        RpcInvocation invocation = new RpcInvocation();
        invocation.setMethodName("test");
        invocation.setInvoker(rocketMQInvoker);

        Map<String, Object> contextAttachments = RpcContext.getContext().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(contextAttachments)) {
            invocation.addObjectAttachments(contextAttachments);
        }

        invocation.setInvokeMode(RpcUtils.getInvokeMode(registryUrl, invocation));
        RpcUtils.attachInvocationIdIfAsync(registryUrl, invocation);

        Byte serializationId = CodecSupport.getIDByName(registryUrl.getParameter(SERIALIZATION_KEY, DEFAULT_REMOTING_SERIALIZATION));
        if (serializationId != null) {
            invocation.put(SERIALIZATION_ID_KEY, serializationId);
        }

        Result result = null;
        try {
            result = rocketMQInvoker.doInvoke(invocation);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        Assertions.assertNotNull(result);
    }

}
