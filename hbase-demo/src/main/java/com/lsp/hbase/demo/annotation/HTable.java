package com.lsp.hbase.demo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @description:
 * @version 1.0
 * @Date 2018-11-5 上午10:40:18
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface HTable {
	/**
	 * 表名
	 * @return
	 */
	String name();

	/**
	 * 列族
	 * @return
	 */
	String[] families() default { "data" };
}
