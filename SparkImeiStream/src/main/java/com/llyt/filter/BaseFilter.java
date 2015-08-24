package com.llyt.filter;

import scala.Tuple2;

import com.google.gson.JsonParser;

/** 
 * @author 作者 E-mail:Dengjh@yintong.cn.com 
 * @version 创建时间：2015-8-10 下午4:54:50
 */

public class BaseFilter implements RDDFilter {

	public void doFilter(Object object, RDDFilterChain chain) {
		String line=(String)object;
		JsonParser parser=new JsonParser();
		String header=parser.parse(line).getAsJsonObject().get("header").getAsString();
		String body=parser.parse(line).getAsJsonObject().get("body").getAsString();
		Tuple2<String, String> tuple=new Tuple2<String, String>(header, body);
		chain.doFilter(tuple, chain);
	}
	
}
