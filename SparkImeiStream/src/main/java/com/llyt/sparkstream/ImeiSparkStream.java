package com.llyt.sparkstream;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaStreamingContextFactory;

/**
 * @author 作者 E-mail:Dengjh@yintong.cn.com
 * @version 创建时间：2015-8-5 下午3:03:44
 */

public class ImeiSparkStream extends StreamContext {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		/*if (args.length < 8) {
			System.err.println("Usage: IMEIDataStreaming <zkQuorum> <group> <topics> <numThreads> <checkPointPath> <appName> <duration>");
			System.exit(1);
		}*/
		
		//final String zkQuorum = null; //required
		//final String group = args[1]; //required
		//final String topics = args[2]; //required
		//final String numThreads = args[3]; //required
		//final String checkPointPath=args[4]; //required
		//final String appName=args[5]; //required
		//final Integer duration=Integer.valueOf(args[6]); //required
		//final String max_cores=args[7]; //required
		//final Map<String, Integer> topicMap=new HashMap<String, Integer>();
		//topicMap.put(topics, Integer.valueOf(numThreads));
		
		JavaStreamingContextFactory contextFactory = new JavaStreamingContextFactory() {
			public JavaStreamingContext create() {
				String zkQuorum=p.getProperty("zkQuorum");
				String group =p.getProperty("group");
				String topics=p.getProperty("topics");
				String numThreads=p.getProperty("numThreads");
				String checkPointPath=p.getProperty("checkPointPath");
				String appName=p.getProperty("appName");
				Integer duration=Integer.valueOf(p.getProperty("duration"));
				String max_cores=p.getProperty("max_cores");
				Map<String, Integer> topicMap=new HashMap<String, Integer>();
				topicMap.put(topics, Integer.valueOf(numThreads));
				
				return createContext(zkQuorum, group, topicMap,max_cores,appName,checkPointPath,duration);
			}
		};
		JavaStreamingContext jsc = JavaStreamingContext.getOrCreate(p.getProperty("checkPointPath"),
				contextFactory);
		jsc.start();
		jsc.awaitTermination();
	}

}
