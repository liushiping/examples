package com.lsp.hbase.demo;

/**
 * 
 * @description: 
 * @version 1.0
 * @Date 2018-11-5 上午11:09:38
 */
public class HTableInfo {
	private String tableName;
	private String[] families;
	private Class<?> refCls;

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String[] getFamilies() {
		return families;
	}

	public void setFamilies(String[] families) {
		this.families = families;
	}

	public Class<?> getRefCls() {
		return refCls;
	}

	public void setRefCls(Class<?> refCls) {
		this.refCls = refCls;
	}

}
