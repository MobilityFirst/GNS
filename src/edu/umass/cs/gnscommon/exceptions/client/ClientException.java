
package edu.umass.cs.gnscommon.exceptions.client;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.GNSException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync.ReconfigurationException;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket.ResponseCodes;


public class ClientException extends GNSException {
	private static final long serialVersionUID = 6627620787610127842L;


	public ClientException(ResponseCode code, String message) {
		super(code, message);
	}


	protected ClientException() {
		super();
	}


	public ClientException(String message, Throwable cause) {
		super(getCode(cause), message, cause);
	}


	public ClientException(String message) {
		super(message);
	}


	public ClientException(Throwable throwable) {
		super(getCode(throwable), throwable);
	}

	// FIXME: Make enums for these static mappings
	private static ResponseCode getCode(Throwable e) {
		if (e instanceof IOException || e.getCause() instanceof IOException)
			return ResponseCode.IO_EXCEPTION;
		if (e instanceof InternalRequestException || e.getCause() instanceof InternalRequestException)
			return ResponseCode.INTERNAL_REQUEST_EXCEPTION;
		if(e instanceof TimeoutException || e.getCause() instanceof TimeoutException) 
			return ResponseCode.TIMEOUT;

		if (e instanceof ReconfigurationException)
			// => none of the above occurred
			return ((ReconfigurationException) e).getCode() == ResponseCodes.DUPLICATE_ERROR ? ResponseCode.DUPLICATE_ID_EXCEPTION

					: ((ReconfigurationException) e).getCode() == ResponseCodes.NONEXISTENT_NAME_ERROR ? ResponseCode.NONEXISTENT_NAME_EXCEPTION

							: ((ReconfigurationException) e).getCode() == ResponseCodes.TIMEOUT_EXCEPTION ? ResponseCode.TIMEOUT
									
							: ResponseCode.RECONFIGURATION_EXCEPTION;
		
		return ResponseCode.UNSPECIFIED_ERROR;
	}


	public ClientException(ResponseCode code, String message, Throwable cause) {
		super(code, message, cause);
	}

}
