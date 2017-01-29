package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class RemoteQueryException extends ClientException {

  private static final long serialVersionUID = 1L;


	public RemoteQueryException(ResponseCode code, String message) {
		super(code, message);
	}


	public RemoteQueryException(String message) {
		super(ResponseCode.REMOTE_QUERY_EXCEPTION, message);
	}

}
