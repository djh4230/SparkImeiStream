package com.llyt.sparkstream;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.llyt.filter.RDDFilter;
import com.llyt.filter.RDDFilterChain;

/**
 * @author 作者 E-mail:Dengjh@yintong.cn.com
 * @version 创建时间：2015-8-5 下午2:35:24
 */

public abstract class StreamContext implements Serializable {

	public static Properties p=null;
	static{
		InputStream is = StreamContext.class.getClassLoader()
				.getResourceAsStream("spark.properties");
		p = new Properties();
		try {
			p.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (is != null) {
				try {
					is.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public static RDDFilterChain getChain() {
		RDDFilterChain chain = new RDDFilterChain();
		String filter = p.getProperty("chain.filters");
		String[] filters = filter.split(",");
		for (String f : filters) {
			try {
				Class c = Class.forName(f);
				RDDFilter rddfilter = (RDDFilter) c.newInstance();
				chain.add(rddfilter);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		return chain;
	}

	public static final JavaStreamingContext createContext(String zkQuorum,
			String group, Map<String, Integer> map, String max_cores,
			String appName, String checkPointPath, Integer duration) {

		SparkConf conf = new SparkConf().setAppName(appName).set(
				"spark.cores.max", max_cores);
		conf.set("spark.serializer",
				"org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.scheduler.pool", "production");
		conf.set("spark.scheduler.mode", "FAIR");
		conf.set("spark.streaming.receiver.writeAheadLog.enable", "true");
		JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(
				duration));
		jsc.checkpoint(checkPointPath);

		final RDDFilterChain chain = getChain();
		JavaPairReceiverInputDStream<String, String> messages = KafkaUtils
				.createStream(jsc, zkQuorum, group, map,
						StorageLevel.MEMORY_AND_DISK_SER());
		messages.checkpoint(new Duration(duration));
		//messages.repartition(3);
		
		JavaPairDStream<String, String> newMessages=messages.filter(new Function<Tuple2<String,String>, Boolean>() {
			public Boolean call(Tuple2<String, String> v1) throws Exception {
				return true;
			}
			
		});
		JavaDStream<String> lines = newMessages
				.map(new Function<Tuple2<String, String>, String>() {
					public String call(Tuple2<String, String> tuple2)
							throws Exception {
						System.out
								.println("---------------------------------------");
						// System.out.println("topic:" + tuple2._1());
						// System.out.println("message:" + tuple2._2());
						String line = tuple2._2();
						// String newLine=PreHandle(line);
						chain.doFilter(line, chain);
						return tuple2._2();
					}
				});

		// lines.print();
		lines.foreach(new Function<JavaRDD<String>, Void>() {

			public Void call(JavaRDD<String> v1) throws Exception {
				List<String> strs = v1.collect();
				System.out.println("foreach:" + strs.size());
				for (String str : strs) {
					System.out.println(str);
				}
				return null;
			}

		});
		return jsc;
	}
	// public abstract String PreHandle(String line);

}
