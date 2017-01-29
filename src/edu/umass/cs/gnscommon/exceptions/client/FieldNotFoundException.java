
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class FieldNotFoundException extends ClientException {
	private static final long serialVersionUID = 2676899572105162853L;


	public FieldNotFoundException(ResponseCode code, String message) {
		super(code, message);
	}


	public FieldNotFoundException(String message) {
		super(ResponseCode.FIELD_NOT_FOUND_EXCEPTION, message);
	}
}
