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

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

/**
 * @author V. Arun
 */
public class ReconfigurationPacketDemultiplexer extends
		AbstractJSONPacketDemultiplexer {
	/**
	 * 
	 */
	public ReconfigurationPacketDemultiplexer(){}
	/**
	 * @param numThreads
	 */
	public ReconfigurationPacketDemultiplexer(int numThreads) {
		super(numThreads);
	}

	@Override
	public boolean handleMessage(JSONObject json) {
		throw new RuntimeException(
				"This method should never be called unless we have \"forgotten\" to register or handle some packet types.");
	}

}
