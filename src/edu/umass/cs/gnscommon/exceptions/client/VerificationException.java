
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class VerificationException extends ClientException {

	private static final long serialVersionUID = 1L;


	public VerificationException(ResponseCode code, String message) {
		super(code, message);
	}


	public VerificationException(String message) {
		super(ResponseCode.VERIFICATION_ERROR, message);
	}
}
