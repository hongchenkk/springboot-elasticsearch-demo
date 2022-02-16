package com.godink.springboot.elasticsearch.demo.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * es配置类
 * @author Hong.Chen
 *
 */
@Configuration
public class ElasticsearchConfig {
	
	@Value("${godink.elasticsearch.host-list}")
	private String esHostList;
	
	//一般经常使用：高级别的api
	@Bean
	public RestHighLevelClient restHighLevelClient() {
		return new RestHighLevelClient(RestClient.builder(getHttpHostList(esHostList)));
	}
	
	//低级别api，少使用，在高级别不满足的时候用
	@Bean
	public RestClient restClient() {
		return RestClient.builder(getHttpHostList(esHostList)).build();
	}

	//解析字符串es的ip:port数组列表
	private HttpHost[] getHttpHostList(String hostList) {
		String[] hosts = hostList.split(",");
		HttpHost[] httpHostArr = new HttpHost[hosts.length];
		for (int i = 0; i < hosts.length; i++) {
			String[] items = hosts[i].split(":");
			httpHostArr[i] = new HttpHost(items[0], Integer.parseInt(items[1]), "http");
		}
		return httpHostArr;
	} 
}
