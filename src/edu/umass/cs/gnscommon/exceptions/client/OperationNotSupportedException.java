
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class OperationNotSupportedException extends ClientException {

	private static final long serialVersionUID = 1L;


	public OperationNotSupportedException(ResponseCode code, String message) {
		super(code, message);
	}


	public OperationNotSupportedException(String message) {
		super(ResponseCode.OPERATION_NOT_SUPPORTED, message);
	}
}
