package com.llyt.filter;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Map;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import scala.Tuple2;

import com.llyt.util.HbaseUtil;

/**
 * @author 作者 E-mail:Dengjh@yintong.cn.com
 * @version 创建时间：2015-8-6 上午9:15:27
 */

public class ForeachFilter implements RDDFilter {

	public void doFilter(Object object, RDDFilterChain chain) {
		Tuple2<String, Map<String, Integer>> tuple2 = (Tuple2<String, Map<String, Integer>>) object;
		HbaseUtil hbaseUtil = HbaseUtil.getInstance("imei", "imei");

		String key = null;
		Map<String, Integer> map = null;
		key = tuple2._1;
		map = tuple2._2;
		try {
			hbaseUtil.put1(key, map);
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			chain.doFilter(null, chain);
		}
	}

}
