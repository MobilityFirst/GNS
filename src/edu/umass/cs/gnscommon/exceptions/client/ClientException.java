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

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.GNSException;

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
		super(message, cause);
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
		super(throwable);
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
