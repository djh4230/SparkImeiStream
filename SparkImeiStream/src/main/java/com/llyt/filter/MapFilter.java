package com.llyt.filter;

import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;


/** 
 * @author 作者 E-mail:Dengjh@yintong.cn.com 
 * @version 创建时间：2015-8-5 下午5:07:17
 * @param <T>
 */

public class MapFilter implements RDDFilter {

	public void doFilter(Object object, RDDFilterChain chain) {
		String line=((Tuple2<String, String>)object)._2;
		String[] items=line.split("\t");
		String imeiID=items[0];  //get imei id
		String imeiData=items[2]; //get imei content
		String[] contents=imeiData.split(" "); //items
		Map<String, Integer> result=new HashMap<String, Integer>();
		for(String content:contents){
			String[] keyWord=content.split("\\|");
			String[] keys=keyWord[0].split("/");
			int value=Integer.valueOf(keyWord[1]);
			int j=0;
			for(String key:keys){
				if(result.containsKey(key)){
					j=result.get(key);
					result.put(key, value+j);
				}
				result.put(key, value+j);
				//System.out.println("key:------"+key);
				//System.out.println("value:--------"+value);
			}
		}
		
		Tuple2<String, Map<String, Integer>> tuple2=new Tuple2<String, Map<String,Integer>>(imeiID, result);
		chain.doFilter(tuple2, chain);
	}
}
