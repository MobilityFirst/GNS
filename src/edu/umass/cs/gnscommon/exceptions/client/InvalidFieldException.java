
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class InvalidFieldException extends ClientException
{
  private static final long serialVersionUID = 2676899572105162853L;


	public InvalidFieldException(ResponseCode code, String message) {
		super(code, message);
	}


	public InvalidFieldException(String message) {
		super(ResponseCode.DUPLICATE_FIELD_EXCEPTION, message);
	}

}
