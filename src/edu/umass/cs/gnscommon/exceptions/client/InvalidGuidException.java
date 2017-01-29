
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class InvalidGuidException extends ClientException {
	private static final long serialVersionUID = 4263493664073760147L;


	public InvalidGuidException(ResponseCode code, String message) {
		super(code, message);
	}


	public InvalidGuidException(String message) {
		super(ResponseCode.BAD_GUID_ERROR, message);
	}
}
