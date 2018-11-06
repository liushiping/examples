package com.lsp.hbase.demo.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 
 * @description：
 * @version  1.0
 * @Date	 2018-11-5 上午10:21:32
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Column {
	
	 /**
	  * 列族名
	  * @return
	  */
     String family() default "";
     
}
