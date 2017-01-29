
package edu.umass.cs.gnscommon.exceptions.server;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.GNSException;


public class ServerException extends GNSException {
	private static final long serialVersionUID = 6627620787610127842L;


	public ServerException() {
		super();
	}


	public ServerException(ResponseCode code, String message) {
		super(code, message);
	}


	public ServerException(String message, Throwable cause) {
		super(message, cause);
	}


	public ServerException(String message) {
		super(message);
	}


	public ServerException(Throwable throwable) {
		super(throwable);
	}

}
