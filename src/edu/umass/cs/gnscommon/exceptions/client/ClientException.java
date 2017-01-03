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
 * Initial developer(s): Westy, Emmanuel Cecchet */
package edu.umass.cs.gnscommon.exceptions.client;

import java.io.IOException;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.GNSException;
import edu.umass.cs.gnscommon.exceptions.server.InternalRequestException;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync.ReconfigurationException;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket.ResponseCodes;

/**
 * This class defines a GnrsException
 * 
 * @author arun
 * @version 1.0
 */
public class ClientException extends GNSException {
	private static final long serialVersionUID = 6627620787610127842L;

	/**
	 * @param code
	 * @param message
	 */
	public ClientException(ResponseCode code, String message) {
		super(code, message);
	}

	/**
	 * Creates a new ClientException.
	 */
	protected ClientException() {
		super();
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 */
	public ClientException(String message, Throwable cause) {
		super(getCode(cause), message, cause);
	}

	/**
	 * 
	 * @param message
	 */
	public ClientException(String message) {
		super(message);
	}

	/**
	 * 
	 * @param throwable
	 */
	public ClientException(Throwable throwable) {
		super(getCode(throwable), throwable);
	}

	private static ResponseCode getCode(Throwable e) {
		if (e instanceof IOException)
			return ResponseCode.IO_EXCEPTION;
		if (e instanceof InternalRequestException)
			return ResponseCode.INTERNAL_REQUEST_EXCEPTION;
		if (e instanceof ReconfigurationException)
			return ((ReconfigurationException) e).getCode() == ResponseCodes.DUPLICATE_ERROR ? ResponseCode.DUPLICATE_ID_EXCEPTION

					: ((ReconfigurationException) e).getCode() == ResponseCodes.NONEXISTENT_NAME_ERROR ? ResponseCode.NONEXISTENT_NAME_EXCEPTION

							: ResponseCode.RECONFIGURATION_EXCEPTION;
		
		return ResponseCode.UNSPECIFIED_ERROR;
	}

	/**
	 * @param code
	 * @param message
	 * @param cause
	 */
	public ClientException(ResponseCode code, String message, Throwable cause) {
		super(code, message, cause);
	}

}
