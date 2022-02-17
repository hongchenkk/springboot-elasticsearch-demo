package com.godink.springboot.elasticsearch.demo;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * elasticsearch测试类: 测试基本的操作
 * 参考文档：https://juejin.cn/post/6844903828932804615
 * @author Hong.Chen
 *
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestESRestClient {
	
	@Autowired
	private RestHighLevelClient restHighLevelClient;//高级别api
	
	@Autowired
	private RestClient restClient;//低级别api

	/**
	 * 测试是否能启动和连通
	 */
	@Test
	void test() {
		System.out.println(restHighLevelClient);
		System.out.println(restClient);
	}
	
	/**
	 * 创建索引库：godink_course
	 * @throws IOException 
	 */
	@Test
	public void testCreateIndex() throws IOException {
		/**
		{
			"settings":{
				"index":{
					"number_of_shards":1,
					"number_of_replicas":0
				}
			}
		}
		 */
		CreateIndexRequest createIndexRequest = new CreateIndexRequest();
		createIndexRequest.index("godink_course");
		createIndexRequest.settings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas",0));
		IndicesClient indicesClient = restHighLevelClient.indices();//通过ES连接对象获取索引库管理对象
		CreateIndexResponse response = indicesClient.create(createIndexRequest);
		System.out.println(response);
		System.out.println(response.isAcknowledged());
	}
	
	/**
	 * 删除索引: godink_course
	 * @throws IOException
	 */
	@Test
	public void testDeleteIndex() throws IOException {
		DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("godink_course");
		IndicesClient indicesClient = restHighLevelClient.indices();
		DeleteIndexResponse response = indicesClient.delete(deleteIndexRequest);
		System.out.println(response);
		System.out.println(response.isAcknowledged());
	}
	
	/**
	 * 创建索引库时指定映射
	 * @throws IOException 
	 */
	@Test
	public void testCreateIndexWithMapping() throws IOException {
		CreateIndexRequest createIndexRequest = new CreateIndexRequest();
		createIndexRequest.index("godink_course");//设置索引库名
		createIndexRequest.settings(Settings.builder().put("number_of_shards", 1).put("number_of_replicas", 0));//设置索引参数
		String mappingJson = "{\r\n"
				+ "    \"properties\": {\r\n"
				+ "        \"name\": {\r\n"
				+ "            \"type\": \"text\",\r\n"
				+ "            \"analyzer\": \"ik_max_word\",\r\n"
				+ "            \"search_analyzer\": \"ik_smart\"\r\n"
				+ "        },\r\n"
				+ "        \"price\": {\r\n"
				+ "            \"type\": \"scaled_float\",\r\n"
				+ "            \"scaling_factor\": 100\r\n"
				+ "        },\r\n"
				+ "        \"timestamp\": {\r\n"
				+ "            \"type\": \"date\",\r\n"
				+ "            \"format\": \"yyyy-MM-dd HH:mm:ss||yyyy-MM-dd\"\r\n"
				+ "        }\r\n"
				+ "    }\r\n"
				+ "}";
		createIndexRequest.mapping("doc", mappingJson, XContentType.JSON);//设置类型，映射结构
		IndicesClient indicesClient = restHighLevelClient.indices();//通过ES连接对象获取索引管理器
		CreateIndexResponse response = indicesClient.create(createIndexRequest);
		System.out.println(response);
		System.out.println(response.isAcknowledged());
	}
	
	/**
	 * 添加文档
	 * @throws IOException 
	 */
	@Test
	public void testAddDocument() throws IOException {
		//构造参数对象
		Map<String, Object> dataMap = new HashMap<>();
		dataMap.put("name", "Java核心技术");
		dataMap.put("price", 66.6);
		dataMap.put("timestamp", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));
		
		IndexRequest indexRequest = new IndexRequest();
		indexRequest.index("godink_course");
		indexRequest.type("doc");
		indexRequest.source(dataMap);
		
		IndexResponse indexResponse = restHighLevelClient.index(indexRequest);
		System.out.println(indexResponse);
	}
	
	/**
	 * 通过文档id查询
	 * @throws IOException 
	 */
	@Test
	public void testFindById() throws IOException {
		GetRequest getRequest = new GetRequest("godink_course", "doc", "WY1fAX8Bo0gLq4HBsk8D");
		GetResponse getResponse = restHighLevelClient.get(getRequest);
		System.out.println(getResponse);
	}
	
	/**
	 * 全量更新的要用rest api
	 * 测试局部更新文档：java客户端提供的是局部替换，仅对提交的字段进行替换
	 * @throws IOException 
	 */
	@Test
	public void testUpdateDoc() throws IOException {
		UpdateRequest updateRequest = new UpdateRequest("godink_course", "doc", "WY1fAX8Bo0gLq4HBsk8D");
		
		Map<String, Object> updateMap = new HashMap<>();
		updateMap.put("name", "Spring核心技术");
		updateMap.put("price", 99.8);
		
		updateRequest.doc(updateMap);
		
		UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
		System.out.println(updateResponse);   
		
		testFindById();
	}
	
	/**
	 * 根据文档id删除文档
	 * @throws IOException 
	 */
	@Test
	public void testDelDoc() throws IOException {
		DeleteRequest deleteRequest = new DeleteRequest("godink_course", "doc", "WY1fAX8Bo0gLq4HBsk8D");
		DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
		System.out.println(deleteResponse);
	}

}
