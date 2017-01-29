
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import org.json.JSONException;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;


public class CommandResponse {


	private final String returnValue;

	private final ResponseCode errorCode;


	public CommandResponse(ResponseCode errorCode) {
		this(errorCode, errorCode.getProtocolCode());
	}


	// Full returnValue strings (second arg) are used by the HTTP server and
	// also retain backward compatibility with older clients.
	public CommandResponse(ResponseCode errorCode, String returnValue) {
		this.returnValue = returnValue;
		this.errorCode = errorCode;
	}


	public String getReturnValue() {
		return returnValue;
	}


	public ResponseCode getExceptionOrErrorCode() {
		return errorCode;
	}


	public static CommandResponse toCommandResponse(Throwable e) {
		if (e instanceof JSONException)
			return new CommandResponse(ResponseCode.JSON_PARSE_ERROR,
					GNSProtocol.BAD_RESPONSE.toString() + " "
							+ GNSProtocol.JSON_PARSE_ERROR.toString() + " "
							+ e.getMessage());
		if (e instanceof ClientException)
			return new CommandResponse(((ClientException) e).getCode(),
					GNSProtocol.BAD_RESPONSE + " "
							+ ((ClientException) e).getMessage()
							+ (e.getCause() != null ? e.getCause() : ""));
		return new CommandResponse(ResponseCode.UNSPECIFIED_ERROR,
				GNSProtocol.BAD_RESPONSE.toString() + " "
						+ GNSProtocol.UNSPECIFIED_ERROR.toString() + " "
						+ e.getMessage());

	}
	

	public static CommandResponse noError() {
		return new CommandResponse(ResponseCode.NO_ERROR,
				GNSProtocol.OK_RESPONSE.toString());		
	}
}
