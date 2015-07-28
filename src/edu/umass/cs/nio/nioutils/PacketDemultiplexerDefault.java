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
package edu.umass.cs.nio.nioutils;

import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;

/**
 * @author V. Arun
 * 
 *         Default packet multiplexer that is a no-op. This class is used 
 *         either for testing or as a trivial multiplexer starting point
 *         upon which more packet multiplexing functions could be built.
 */
public class PacketDemultiplexerDefault extends AbstractJSONPacketDemultiplexer {
	@Override
	public final boolean handleMessage(JSONObject jsonObject) {
		//NIOInstrumenter.incrPktsRcvd();
		return false; // must remain false;
	}
}
