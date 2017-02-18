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

/**
 * This class defines a GnsException
 */
public class InternalRequestException extends ServerException {
	private static final long serialVersionUID = 6627620787610127842L;

	/**
	 * @param code
	 * @param message
	 */
	public InternalRequestException(ResponseCode code, String message) {
		super(code, message);
	}

	/**
	 * Creates a new <code>InternalRequestException</code> object
	 * 
	 * @param message
	 */
	public InternalRequestException(String message) {
		super(message);
	}

	/**
	 * Creates a new <code>InternalRequestException</code> object
	 * 
	 * @param throwable
	 */
	public InternalRequestException(Throwable throwable) {
		super(throwable);
	}

}
