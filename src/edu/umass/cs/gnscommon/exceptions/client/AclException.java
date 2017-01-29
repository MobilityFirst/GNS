
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class AclException extends ClientException {
	private static final long serialVersionUID = 7789779657368481702L;


	public AclException(ResponseCode code, String message) {
		super(code, message);
	}


	public AclException(String message) {
		super(ResponseCode.ACCESS_ERROR, message);
	}

}
