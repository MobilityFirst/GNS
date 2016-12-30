/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): Westy, Arun */
package edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport;

import org.json.JSONException;

import edu.umass.cs.gnscommon.GNSProtocol;
import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.client.ClientException;

/**
 * Encapsulates the response string and {@link ResponseCode} that we pass back
 * to the client.
 */
public class CommandResponse {

	/**
	 * Value returned.
	 */
	private final String returnValue;
	/**
	 * Indicates if the response is an error. Can be null.
	 */
	private final ResponseCode errorCode;

	/**
	 * @param errorCode
	 */
	public CommandResponse(ResponseCode errorCode) {
		this(errorCode, errorCode.getProtocolCode());
	}

	/**
	 * Create a command response object from a return value with an error code.
	 *
	 * @param errorCode
	 * @param returnValue
	 *
	 */
	// Full returnValue strings (second arg) are used by the HTTP server and
	// also retain backward compatibility with older clients.
	public CommandResponse(ResponseCode errorCode, String returnValue) {
		this.returnValue = returnValue;
		this.errorCode = errorCode;
	}

	/**
	 * Gets the return value.
	 *
	 * @return a string
	 */
	public String getReturnValue() {
		return returnValue;
	}

	/**
	 * Gets the error code.
	 *
	 * @return a {@link ResponseCode}
	 */
	public ResponseCode getExceptionOrErrorCode() {
		return errorCode;
	}

	/**
	 * @param e
	 * @return CommandResponse created from {@code e}
	 */
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
}
