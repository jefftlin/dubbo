package org.apache.dubbo.rpc.rocketmq;

import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;

import java.util.Map;
import java.util.Objects;
import java.util.zip.CRC32;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.protocol.AbstractExporter;

public class RocketMQExporter<T> extends AbstractExporter<T> {

	private static final String DEFAULT_PARAM_VALUE = "";

	private static final String NAME_SEPARATOR = "_";

	private final String key;

	private final Map<String, Exporter<?>> exporterMap;

	public RocketMQExporter(Invoker<T> invoker, URL url, Map<String, Exporter<?>> exporterMap) {
		super(invoker);
		this.key = toValue(url);
		this.exporterMap = exporterMap;
		this.exporterMap.put(key, this);
	}

	public void afterUnExport() {
		exporterMap.remove(key, this);
	}
	
	public String getKey() {
		return this.key;
	}

	private String toValue(URL url) {
		String serviceInterface = url.getParameter(INTERFACE_KEY);
		String version = url.getParameter(VERSION_KEY, DEFAULT_PARAM_VALUE);
		String group = url.getParameter(GROUP_KEY, DEFAULT_PARAM_VALUE);

		String value = null;
		if (Objects.equals(url.getParameter("groupModel"), "topic")) {
			value = DEFAULT_CATEGORY + NAME_SEPARATOR + serviceInterface + NAME_SEPARATOR + version + NAME_SEPARATOR + group;
		} else {
			value = DEFAULT_CATEGORY + NAME_SEPARATOR + serviceInterface;
		}
		CRC32 crc32 = new CRC32();
		crc32.update(value.getBytes());
		value = value.replace(".", "-")+ NAME_SEPARATOR + Long.toString(crc32.getValue());
		return value;
	}

}
