package com.lsp.hbase.demo.exceptions;

/**
 * 
 * @description:	 TODO add description
 * @version  Ver 1.0
 * @Date	 2018-11-5 上午11:45:11
 */
public class HbaseException extends RuntimeException {
	private static final long serialVersionUID = -5757561588219789043L;

	public HbaseException() {
		super();
	}

	public HbaseException(String message) {
		super(message);
	}

	public HbaseException(Throwable cause) {
		super(cause);
	}

	public HbaseException(String message, Throwable cause) {
		super(message, cause);
	}

	public HbaseException(String message, Throwable cause, String code,
			Object[] values) {
		
	}
}
