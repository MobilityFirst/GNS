
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class EncryptionException extends ClientException {
	private static final long serialVersionUID = 1721392537222462554L;


	public EncryptionException(ResponseCode code, String message) {
		super(code, message);
	}


	public EncryptionException(String message) {
		super(ResponseCode.SIGNATURE_ERROR, message);
	}


	public EncryptionException(String message, Throwable cause) {
		super(ResponseCode.SIGNATURE_ERROR, message, cause);
	}

}
