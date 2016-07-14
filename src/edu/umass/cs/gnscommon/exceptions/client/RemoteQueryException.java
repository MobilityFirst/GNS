package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.GNSResponseCode;

public class RemoteQueryException extends ClientException {

	/**
	 * @param code
	 * @param message
	 */
	public RemoteQueryException(GNSResponseCode code, String message) {
		super(code, message);
	}

	/**
	 * @param message
	 */
	public RemoteQueryException(String message) {
		super(GNSResponseCode.REMOTE_QUERY_EXCEPTION, message);
	}

}
