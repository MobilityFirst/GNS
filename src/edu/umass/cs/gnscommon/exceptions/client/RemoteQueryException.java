package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;

/**
 *
 * @author westy
 */
public class RemoteQueryException extends ClientException {

	/**
	 * @param code
	 * @param message
	 */
	public RemoteQueryException(ResponseCode code, String message) {
		super(code, message);
	}

	/**
	 * @param message
	 */
	public RemoteQueryException(String message) {
		super(ResponseCode.REMOTE_QUERY_EXCEPTION, message);
	}

}
