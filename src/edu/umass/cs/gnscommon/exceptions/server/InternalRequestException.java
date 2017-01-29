
package edu.umass.cs.gnscommon.exceptions.server;

import edu.umass.cs.gnscommon.ResponseCode;


public class InternalRequestException extends ServerException {
	private static final long serialVersionUID = 6627620787610127842L;


	public InternalRequestException(ResponseCode code, String message) {
		super(code, message);
	}


	public InternalRequestException(String message) {
		super(message);
	}


	public InternalRequestException(Throwable throwable) {
		super(throwable);
	}

}
