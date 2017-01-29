
package edu.umass.cs.gnscommon.exceptions.client;

import edu.umass.cs.gnscommon.ResponseCode;


public class DuplicateNameException extends ClientException {


	private static final long serialVersionUID = 5740974385175983703L;



	public DuplicateNameException(ResponseCode code, String message) {
		super(code, message);
	}


	public DuplicateNameException(String message) {
		super(ResponseCode.DUPLICATE_ID_EXCEPTION, message);
	}
}
