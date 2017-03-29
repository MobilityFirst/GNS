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
 * Initial developer(s): Westy */
package edu.umass.cs.gnscommon.exceptions.server;

import edu.umass.cs.gnscommon.ResponseCode;
import edu.umass.cs.gnscommon.exceptions.GNSException;

/**
 * This class defines a ServerException
 */
public class ServerException extends GNSException {
	private static final long serialVersionUID = 6627620787610127842L;

	/**
	 * Creates a new <code>ServerException</code> object
	 */
	public ServerException() {
		super();
	}

	/**
	 * @param code
	 * @param message
	 */
	public ServerException(ResponseCode code, String message) {
		super(code, message);
	}

	/**
	 * Creates a new <code>ServerException</code> object
	 * 
	 * @param message
	 * @param cause
	 */
	public ServerException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Creates a new <code>ServerException</code> object
	 * 
	 * @param message
	 */
	public ServerException(String message) {
		super(message);
	}

	/**
	 * Creates a new <code>ServerException</code> object
	 * 
	 * @param throwable
	 */
	public ServerException(Throwable throwable) {
		super(throwable);
	}

}
