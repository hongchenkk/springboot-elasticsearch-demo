package com.godink.springboot.elasticsearch.demo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * elasticsearch测试类: 测试搜索和查询操作
 * 参考文档：https://juejin.cn/post/6844903828932804615
 *
 * @author Hong.Chen
 *
 */
@SpringBootTest
@RunWith(SpringRunner.class)
public class TestESRestClientSearch {

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
	 * DSL搜索-查询所有文档——matchAllQuery
	 * @throws IOException 
	 */
	@Test
	public void testMatchAll() throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");//设置索引库名
		searchRequest.types("doc");//设置类型名
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//DSL请求体构造对象
	    /**
         * {
         * 	"from":2,"size":1,
         * 	"query":{
         * 		"match_all":{
         *
         *                }* 	},
         * 	"_source":["name","studymodel"]
         * }
         */
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());
		//参数1：要返回哪些字段   参数2：不要返回哪些字段  两者通常指定其一
		searchSourceBuilder.fetchSource(new String[] {"name", "studymodel"}, null);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-分页查询
	 * @throws IOException 
	 */
	@Test
	public void testPage() throws IOException {
		SearchRequest searchRequest = new SearchRequest();//构造搜索请求对象
		searchRequest.indices("godink_search");//设置索引库名
		searchRequest.types("doc");//设置类型名
		
		int page = 1,size =1;
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//构造请求对象的请求体
		searchSourceBuilder.from((page-1)*size);//设置偏移量
		searchSourceBuilder.size(size);//设置查询的个数
		searchSourceBuilder.query(QueryBuilders.matchAllQuery());//设置为查询所有文档
		searchSourceBuilder.fetchSource(new String[] {"name", "studymodel"}, null);//设置要返回的字段

		printResult(searchRequest, searchSourceBuilder);
	}

	//提取结果集中的文档
	private void printResult(SearchRequest searchRequest, SearchSourceBuilder searchSourceBuilder) {
		//将请求体设置请求对象
		searchRequest.source(searchSourceBuilder);
		//通过高级api发起请求
		SearchResponse searchResponse = null;
		try {
			searchResponse = restHighLevelClient.search(searchRequest);
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(searchResponse);
		System.out.println("------------提取结果集文档-->start-------------");
		SearchHits hits = searchResponse.getHits();
		if(hits != null) {
			SearchHit[] docList = hits.getHits();
			for (SearchHit searchHit : docList) {
				System.out.println(searchHit.getSourceAsMap());
			}
		}
	}
	
	/**
	 * 词项精确匹配：不会进行搜索分词
	 * @throws IOException 
	 */
	@Test
	public void testQueryByTerm() throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		int page = 1, size = 1;
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.from((page-1)*size);
		searchSourceBuilder.size(size);
		searchSourceBuilder.query(QueryBuilders.termQuery("name", "java"));
		searchSourceBuilder.fetchSource(new String[] {"name", "studymodel"}, null);
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * 根据id精确匹配——termsQuery：查询id为1和3的文档
	 * @throws IOException 

	 */
	@Test
	public void testQueryByIds() throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		List<String> ids = Arrays.asList(new String[] {"1", "3"});
		searchSourceBuilder.query(QueryBuilders.termsQuery("_id", ids));
		searchSourceBuilder.fetchSource(new String[] {"name", "studymodel"}, null);
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/*
	 * DSL搜索-全文检索(对搜索的词汇进行分词，既搜索分词)---matchQuery
	 */
	@Test
	public void testMatchQuery() throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery("name", "bootstrap基础"));
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索之取交集---matchQuery
	 * @throws IOException 
	 */
	@Test
	public void testMatchQueryAnd() throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery("name", "java基础").operator(Operator.AND));
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索之最小匹配占比-minimum_should_match
	 * @throws IOException 
	 */
	@Test
	public void testMinimumShouldMatch() throws IOException {
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.matchQuery("name", "spring开发框架").minimumShouldMatch("70%"));//3*0.7 -> 2
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-多域检索——multiMatchQuery
	 * @throws IOException 
	 */
	@Test
	public void testMultiMatchQuery() throws IOException {
		SearchRequest searchRequest = new SearchRequest("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		searchSourceBuilder.query(QueryBuilders.multiMatchQuery("spring css", "name", "description").minimumShouldMatch("50%"));
		searchSourceBuilder.fetchSource(new String[] {"name", "description"}, null);
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-设置权重（boost）
	 */
	@Test
	public void testSetupWeight() {
		SearchRequest searchRequest = new SearchRequest("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		MultiMatchQueryBuilder sourceBuilder = QueryBuilders.multiMatchQuery("spring css", "name", "description").minimumShouldMatch("50%");
		sourceBuilder.field("name", 10);
		searchSourceBuilder.query(sourceBuilder);
		searchSourceBuilder.fetchSource(new String[] {"name", "description"}, null);
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-布尔查询——boolQuery
	 * @throws IOException 
	 */
	@Test
	public void testBoolQuery() throws IOException {
		/**
		{
		    "query": {
		        "bool":{
		            "must":[
		                {
		                    "term":{
		                        "name":"spring"
		                    }
		                },
		                {
		                    "multi_match":{
		                        "query":"开发框架",
		                        "fields":["name","description"]
		                    }
		                }
		            ]
		        }
		    },
		    "_source":["name"]
		}
		 */
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();//query
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();//query.bool
		
		TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "spring");
		MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("开发框架", "name", "description");
		
		boolQueryBuilder.must(termQuery);
		boolQueryBuilder.must(multiMatchQuery);
		searchSourceBuilder.query(boolQueryBuilder);
		
		searchSourceBuilder.fetchSource(new String[] {"name", "description"}, null);
		
//		searchRequest.source(searchSourceBuilder);
//		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
//		System.out.println(searchResponse);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-布尔查询——boolQuery-复杂布尔条件
	 */
	@Test
	public void testBoolQueryComplex() {
		/**
		{
		    "query":{
		        "bool":{
		            "must":[
		                {
		                    "term":{
		                        "name":"开发"
		                    }
		                }
		            ],
		            "must_not":[
		                {
		                    "term":{
		                        "name":"java"
		                    }
		                }
		            ],
		            "should":[
		                {
		                    "term":{
		                        "name":"bootstrap"
		                    }
		                },
		                {
		                    "term":{
		                        "name":"spring"
		                    }
		                }
		            ]
		        }
		    }
		}
		 */
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		boolQueryBuilder.must(QueryBuilders.termQuery("name", "开发"));
		boolQueryBuilder.mustNot(QueryBuilders.termQuery("name", "java"));
		boolQueryBuilder.should(QueryBuilders.termQuery("name", "bootstrap"));
		boolQueryBuilder.should(QueryBuilders.termQuery("name", "spring"));
		
		searchSourceBuilder.query(boolQueryBuilder);
		
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-过滤器——filter
	 */
	@Test
	public void testBoolFilterQuery() {
		/**
		{
		    "query":{
		        "bool":{
		            "must":[
		                {
		                    "multi_match":{
		                        "query":"spring框架",
		                        "fields":[
		                            "name",
		                            "description"
		                        ] 
		                    }
		                }
		            ],
		            "filter":[
		                {
		                    "term":{
		                        "studymodel":"201001"
		                    }
		                },
		                {
		                    "range":{
		                        "price":{
		                            "gte":"10",
		                            "lte":"100"
		                        }
		                    }
		                }
		            ]
		        }
		    }
		}
		 */
		
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		
		boolQueryBuilder.must(QueryBuilders.multiMatchQuery("spring框架", "name", "description"));
		boolQueryBuilder.filter(QueryBuilders.termQuery("studymodel", "201001"));
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte("10").lte("100"));
		
		searchSourceBuilder.query(boolQueryBuilder);
		
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-排序
	 */
	@Test
	public void testSort() {
		/**
			{
			    "query":{
			        "bool":{
			            "filter":[
			                {
			                    "range":{
			                        "price":{
			                            "gte":"10",
			                            "lte":"100"
			                        }
			                    }
			                }
			            ]
			        }
			    },
			    "sort":[
			        {
			            "price":"asc"
			        },
			        {
			            "timestamp":"desc"
			        }
			    ],
			    "_source":[
			        "name",
			        "price",
			        "timestamp"
			    ]
			}
		 */
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte("10").lte("100"));
		
		searchSourceBuilder.sort("price", SortOrder.ASC);
		searchSourceBuilder.sort("price", SortOrder.DESC);
		
		searchSourceBuilder.fetchSource(new String[] {"name", "price", "timestamp"}, null); 
		
		searchSourceBuilder.query(boolQueryBuilder);
		printResult(searchRequest, searchSourceBuilder);
	}
	
	/**
	 * DSL搜索-全文检索-高亮
	 * @throws IOException 
	 */
	@Test
	public void testHighlight() throws IOException {
		/**
		{
		    "query":{
		        "bool":{
		            "filter":[
		                {
		                    "multi_match":{
		                        "query":"bootstrap 开发",
		                        "fields":[
		                            "name",
		                            "description"
		                        ]
		                    }
		                }
		            ]
		        }
		    },
		    "highlight":{
		        "pre_tags":["<tag>"],
		        "post_tags":["</tag>"],
		        "fields":{
		            "name":{},
		            "description":{}
		        }
		    }
		}
		 */
		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("godink_search");
		searchRequest.types("doc");
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
		
		boolQueryBuilder.filter(QueryBuilders.multiMatchQuery("bootstrap 开发", "name", "description"));
		
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		highlightBuilder.preTags("<tag>");
		highlightBuilder.postTags("</tag>");
		highlightBuilder.field("name").field("description");
		
		searchSourceBuilder.query(boolQueryBuilder);
		searchSourceBuilder.highlighter(highlightBuilder);
		
		searchRequest.source(searchSourceBuilder);
		SearchResponse searchResponse = restHighLevelClient.search(searchRequest);
		System.out.println(searchResponse);
		
		//结果解析
		SearchHits hits = searchResponse.getHits();//hits
		if(hits != null) {
			SearchHit[] results = hits.getHits();//hits.hits
			if(results != null) {
				for (SearchHit result : results) {
					
					//解析name
					Map<String, Object> source = result.getSourceAsMap();//_source
					String nameFromSource = (String)source.get("name");
					System.out.println("nameFromSource:"+nameFromSource);
					
					Map<String, HighlightField> highlightFields = result.getHighlightFields();//highlight
					HighlightField highlightField = highlightFields.get("name");//highlight.name
					if(highlightField != null) {
						Text[] fragments = highlightField.getFragments();
						StringBuilder stringBuilder = new StringBuilder();
						for (Text text : fragments) {
							stringBuilder.append(text.toString());
						}
						String nameFromHighlight = stringBuilder.toString();
						System.out.println("nameFromHighlight:"+nameFromHighlight);
					}
					
					//解析description
					String descriptionFromSource = (String)source.get("description");
					System.out.println("descriptionFromSource:" + descriptionFromSource);
					
					HighlightField highlightField2 = highlightFields.get("description");
					if(highlightField2 != null) {
						Text[] fragments = highlightField2.getFragments();
						StringBuilder stringBuilder = new StringBuilder();
						for (Text text : fragments) {
							stringBuilder.append(text.toString());
						}
						String descriptionFromHighlight = stringBuilder.toString();
						System.out.println("descriptionFromHighlight:"+descriptionFromHighlight);
					}
				}
			}
		}
	}
}
