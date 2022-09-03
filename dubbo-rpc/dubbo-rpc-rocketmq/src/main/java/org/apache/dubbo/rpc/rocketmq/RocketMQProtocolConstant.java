package org.apache.dubbo.rpc.rocketmq;

import java.net.InetSocketAddress;

import org.apache.dubbo.common.utils.NetUtils;

public interface RocketMQProtocolConstant {

	
	static final String CONSUMER_CROUP_NAME = "dubbo-roucketmq-consumer-group";
	
	static final String PRODUCER_CROUP_NAME = "dubbo-roucketmq-producer-group";
	
	static final String DUBBO_DEFAULT_PROTOCOL_TOPIC = "dubbo_default_protocol_topic"; 

	static final String SEND_ADDRESS = "send_address";
	
	static final String URL_STRING = "url_string";
	
	static final InetSocketAddress LOCAL_ADDRESS = InetSocketAddress.createUnresolved(NetUtils.getLocalHost(),9876);
}
