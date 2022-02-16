package com.godink.springboot.elasticsearch.demo.config;

import java.util.concurrent.Executor;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * 配置类：配置过滤器，bean等
 * @ClassName: WebConfiguration 
 * @author: Hong.Chen
 * @date: 2021年3月24日 上午11:49:07
 */
@Configuration
public class WebConfiguration {
	
    @Bean
    public Executor taskExecutor() {
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		//设置核心线程数
		int cores = Runtime.getRuntime().availableProcessors();
		executor.setCorePoolSize(cores);
		//设置最大线程数
		executor.setMaxPoolSize(20);
		//等待所有任务结束后再关闭线程池
		executor.setWaitForTasksToCompleteOnShutdown(true);
		//设置线程默认前缀名
		executor.setThreadNamePrefix("Becp-Ecms-Async-Task-");
		return executor;
	}
    
    @Bean
    public TaskScheduler taskScheduler() {
    	ThreadPoolTaskScheduler taskScheduler = new ThreadPoolTaskScheduler();
		//设置核心线程数
		int cores = Runtime.getRuntime().availableProcessors();
		taskScheduler.setPoolSize(cores * 3);
		//等待所有任务结束后再关闭线程池
		taskScheduler.setWaitForTasksToCompleteOnShutdown(true);
		//设置线程默认前缀名
		taskScheduler.setThreadNamePrefix("Becp-Ecms-Scheduler-Task-");
    	return taskScheduler;
    }
}
