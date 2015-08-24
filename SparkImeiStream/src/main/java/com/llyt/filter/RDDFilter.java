package com.llyt.filter;

import java.io.Serializable;


/** 
 * @author 作者 E-mail:Dengjh@yintong.cn.com 
 * @version 创建时间：2015-8-5 下午4:36:18
 * @param <T>
 */

public interface RDDFilter extends Serializable{
	void doFilter(Object object, RDDFilterChain chain);
}
