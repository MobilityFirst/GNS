/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.interfaces;

import java.util.Set;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author V. Arun
 */
public interface Application {
	/**
	 * @param request
	 * @return True if the request is executed successfully. 
	 */
	public boolean execute(Request request);

	/**
	 * App must support string-to-InterfaceRequest conversion and back.
	 * Furthermore, the conversion to a string and back must preserve the return
	 * values of all InterfaceRequest methods, i.e.,
	 * {@code InterfaceApplication.getRequest(request.toString())).getRequestType =
	 * request.getRequestType()} ... and so on
	 * 
	 * @param stringified
	 * @return InterfaceRequest corresponding to {@code stringified}.
	 * @throws RequestParseException
	 */
	public Request getRequest(String stringified) throws RequestParseException;
	
	/**
	 * @return The set of request types that the application expects to process.
	 */
	public Set<IntegerPacketType> getRequestTypes();
}
