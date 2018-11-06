package com.lsp.hbase.demo.model;

import java.io.Serializable;

public abstract class HbaseBaseModel implements Serializable{
 
	private static final long serialVersionUID = -1473432240246883061L;

	public abstract String[] getColumns();
	
	public abstract String familyName();
	
}
