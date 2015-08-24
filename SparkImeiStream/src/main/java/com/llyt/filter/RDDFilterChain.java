package com.llyt.filter;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.shell.Count;
import org.apache.spark.api.java.JavaRDD;

/** 
 * @author 作者 E-mail:Dengjh@yintong.cn.com 
 * @version 创建时间：2015-8-5 下午4:48:50
 * @param <T>
 */

public class RDDFilterChain implements RDDFilter{

	List<RDDFilter> filters=new ArrayList<RDDFilter>();
	private int index=0;
	
	public RDDFilterChain add(RDDFilter filter){
		filters.add(filter);
		return this;
	}
	
	public void doFilter(Object object, RDDFilterChain chain) {
		if(index==filters.size()){
			index=0;
			return;
		}
		filters.get(index++).doFilter(object, chain);
	}
	
}
