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
package edu.umass.cs.protocoltask.examples;

import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.protocoltask.ProtocolExecutor;

/**
 * @author V. Arun
 */
public class ExamplePacketDemultiplexer extends AbstractJSONPacketDemultiplexer {
	private static final boolean DEBUG = true;
	private final ExampleNode node;
	private Logger log =
			ProtocolExecutor.getLogger();

	ExamplePacketDemultiplexer(ExampleNode n) {
		this.node = n;
		this.register(PingPongPacket.PacketType.TEST_PING);
		this.register(PingPongPacket.PacketType.TEST_PONG);
	}

	@Override
	public boolean handleMessage(JSONObject json) {
		try {
			if (DEBUG)
				log.finest("PD " + this.node.getMyID() + " received " + json);
			switch (PingPongPacket.PacketType.intToType.get(JSONPacket.getPacketType(json))) {
			case TEST_PING:
			case TEST_PONG:
				this.node.handleIncoming(json);
				break;
			default:
				return false;
			}
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return true;
	}

}
