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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Application;

/**
 * @author V. Arun
 * 
 */
public class RequestParseException extends Exception {
	static final long serialVersionUID = 0;

	/**
	 * Meant to be thrown when {@link Application#getRequest(String)}
	 * can not parse the supplied string into an InterfaceRequest object.
	 * 
	 * @param e
	 */
	public RequestParseException(Exception e) {
		super(e);
	}

	static void main(String[] args) {
		JSONObject json = new JSONObject();
		try {
			json.getString("key");
		} catch (JSONException je) {
			RequestParseException rpe = new RequestParseException(je);
			rpe.printStackTrace();
		}
	}
}
