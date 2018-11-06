package com.lsp.hbase.demo.commons;

/**
 * @description:平台常量
 * @version  1.0
 * @Date	 2018-11-5 下午5:55:44
 */
public abstract class PlatFormConstants {
	
	/**
	 * 日志收集分隔符号
	 */
	public final static String LOG_SPLIT_SIGN = "|";
	
	/**
	 * hbase zookeeper 集群
	 */
	public static final String HBASE_ZOOKEEPER_QUORUM= "hbase.zookeeper.quorum";
	
	/**
	 * hbase在hadoop上的根目录
	 */
	public static final String HBASE_ROOTDIR = "hbase.rootdir";
	
	
//	public static String getHbaseZooQuoRum() {
//		return  ConfigUtils.get(HBASE_ZOOKEEPER_QUORUM);
//	}
//	
//	public static String getHbaseRootDir() {
//		return  ConfigUtils.get(HBASE_ROOTDIR);
//	}
	
}
