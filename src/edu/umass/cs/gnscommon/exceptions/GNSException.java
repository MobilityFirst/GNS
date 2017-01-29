package edu.umass.cs.gnscommon.exceptions;

import edu.umass.cs.gnscommon.ResponseCode;


public class GNSException extends Exception {

	final ResponseCode code;

	private static final long serialVersionUID = 6816831396928147083L;


	protected GNSException() {
		super();
		this.code = null;
	}


	public GNSException(ResponseCode code, String message, String GUID) {
		super(message);
		this.code = code;
	}


	public GNSException(ResponseCode code, String message) {
		this(code, message, (String) null);
	}


	public GNSException(String message, Throwable cause) {
		super(message, cause);
		this.code = null;
	}


	public GNSException(String message) {
		this(null, message);
	}


	public GNSException(Throwable throwable) {
		super(throwable);
		this.code = null;
	}


	public GNSException(ResponseCode code, String message, Throwable cause) {
		super(message, cause);
		this.code = code;
	}


	public GNSException(ResponseCode code, Throwable throwable) {
		super(throwable);
		this.code = code;
	}


	public ResponseCode getCode() {
		return this.code;
	}
}
