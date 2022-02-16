package com.godink.springboot.elasticsearch.demo.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@RestController
public class HelloWorldController {

	@GetMapping("/hello")
	public String index() {
		return "Hello World";
	}
	
	@GetMapping("/user")
	public Map<String, Object> getUser() {
		Map<String, Object> result = new HashMap<>();
		result.put("name", "ch");
		result.put("age", "12");
		result.put("sex", "1");
		result.put("like", "爱唱歌");
		log.info("getUser: {}", result);
		return result;
	}
	
}
