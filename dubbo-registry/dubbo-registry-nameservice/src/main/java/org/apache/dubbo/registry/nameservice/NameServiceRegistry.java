package org.apache.dubbo.registry.nameservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.constants.RegistryConstants;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;

public class NameServiceRegistry extends FailbackRegistry {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	private ScheduledExecutorService scheduledExecutorService;

	private Map<URL, RegistryInfoWrapper> consumerRegistryInfoWrapperMap = new ConcurrentHashMap<>();

	private MQClientInstance client;

	private boolean isNotRoute = true;

	private ClusterInfo clusterInfo;

	private TopicList topicList;

	private long timeoutMillis;
	
	private String instanceName;

	public NameServiceRegistry(URL url) {
		super(url);
		this.isNotRoute = !url.getParameter("route", false);
		if (this.isNotRoute) {
			return;
		} 
		this.timeoutMillis = url.getParameter("timeoutMillis", 3000);
		this.instanceName = url.getParameter("instanceName");
		ClientConfig clientConfig = new ClientConfig();
		clientConfig.setNamesrvAddr(  url.getAddress());
		clientConfig.setInstanceName(instanceName);
		client = MQClientManager.getInstance().getOrCreateMQClientInstance(clientConfig);
		try {
			client.start();
			this.initBeasInfo();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r, "dubbo-registry-nameservice");
			}
		});
		scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				try {
					NameServiceRegistry.this.initBeasInfo();
					
					if (consumerRegistryInfoWrapperMap.isEmpty()) {
						return;
					}
					for (Entry<URL, RegistryInfoWrapper> e : consumerRegistryInfoWrapperMap.entrySet()) {
						List<URL> urls = new ArrayList<URL>();
						NameServiceRegistry.this.pullRoute(e.getValue().serviceName, e.getKey(), urls);
						e.getValue().listener.notify(urls);
					}
				} catch (Exception e) {
					logger.error("ScheduledTask pullRoute exception", e);
				}
			}
		}, 1000 * 10, 3000 * 10, TimeUnit.MILLISECONDS);
	}

	private void initBeasInfo() throws Exception {
		this.clusterInfo = this.client.getMQClientAPIImpl().getBrokerClusterInfo(timeoutMillis);
		this.topicList = this.client.getMQClientAPIImpl().getTopicListFromNameServer(timeoutMillis);
	}

	private URL createProviderURL(ServiceName serviceName, URL url, int queue) {
		Map<String, String> parameters = url.getParameters();
		parameters.put(CommonConstants.INTERFACE_KEY, serviceName.getServiceInterface());
		parameters.put(CommonConstants.PATH_KEY, serviceName.getServiceInterface());
		parameters.put("bean.name", "ServiceBean:" + serviceName.getServiceInterface());
		parameters.put(CommonConstants.SIDE_KEY, CommonConstants.PROVIDER);
		parameters.put(RegistryConstants.CATEGORY_KEY, "providers");
		parameters.put(CommonConstants.PROTOCOL_KEY, "rocketmq");
		parameters.put("queueId", queue+"");
		parameters.put("topic", serviceName.getValue());
		return new URL("rocketmq",this.getUrl().getIp(),this.getUrl().getPort(),url.getPath(),parameters);
	}
	
	private ServiceName createServiceName(URL url) {
		return new ServiceName(url);
	}

	private boolean createTopic(ServiceName serviceName) {
		if (!this.topicList.getTopicList().contains(serviceName.getValue())) {
			for (Entry<String, BrokerData> entry : this.clusterInfo.getBrokerAddrTable().entrySet()) {
				String brokerArr = entry.getValue().getBrokerAddrs().get(MixAll.MASTER_ID);
				try {
					TopicConfig topicConfig = new TopicConfig(serviceName.getValue());
					topicConfig.setReadQueueNums(8);
					topicConfig.setWriteQueueNums(8);
					this.client.getMQClientAPIImpl().createTopic(brokerArr, null, topicConfig, timeoutMillis);
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
			return true;
		} else {
			return false;
		}

	}

	@Override
	public boolean isAvailable() {
		return false;
	}

	@Override
	public void doRegister(URL url) {
	}

	@Override
	public void doUnregister(URL url) {
	}

	@Override
	public void doSubscribe(URL url, NotifyListener listener) {
		List<URL> urls = new ArrayList<URL>();
		ServiceName serviceName = this.createServiceName(url);
		if (this.isNotRoute) {
			URL providerURL = this.createProviderURL(serviceName, url, -1);
			urls.add(providerURL);
		} else {
			RegistryInfoWrapper registryInfoWrapper = new RegistryInfoWrapper();
			registryInfoWrapper.listener = listener;
			registryInfoWrapper.serviceName = serviceName;
			consumerRegistryInfoWrapperMap.put(url, registryInfoWrapper);
			this.pullRoute(serviceName, url, urls);
		}
		listener.notify(urls);
	}

	void pullRoute(ServiceName serviceName, URL url, List<URL> urls) {
		try {
			this.createTopic(serviceName);
			String topic = serviceName.getValue();
			TopicRouteData topicRouteData = this.client.getMQClientAPIImpl().getTopicRouteInfoFromNameServer(topic,
					this.timeoutMillis);

			Map<String, String> brokerAddrBybrokerName = new HashMap<>();
			for (BrokerData brokerData : topicRouteData.getBrokerDatas()) {
				brokerAddrBybrokerName.put(brokerData.getBrokerName(), brokerData.selectBrokerAddr());
			}
			for (QueueData queueData : topicRouteData.getQueueDatas()) {
				if (PermName.isReadable(queueData.getPerm())) {
					for (int i = 0; i < queueData.getReadQueueNums(); i++) {
						URL newUrl = this.createProviderURL(serviceName, url, i);
						urls.add(newUrl.addParameter("brokerName", queueData.getBrokerName()));
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void doUnsubscribe(URL url, NotifyListener listener) {
		this.consumerRegistryInfoWrapperMap.remove(url);
	}

	private class RegistryInfoWrapper {

		private NotifyListener listener;

		private ServiceName serviceName;
	}
}
