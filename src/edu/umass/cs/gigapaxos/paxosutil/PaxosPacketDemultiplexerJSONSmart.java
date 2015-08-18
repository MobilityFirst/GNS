/*
 * Copyright (c) 2015 University of Massachusetts
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
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.gigapaxos.paxosutil;

import java.util.Collection;

import net.minidev.json.JSONValue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket.PaxosPacketType;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.MessageExtractor;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;

/**
 * @author V. Arun
 *         <p>
 *         Used to get NIO to send paxos packets to PaxosManager. This class has
 *         been merged into PaxosManager now and will be soon deprecated.
 */
public abstract class PaxosPacketDemultiplexerJSONSmart extends
		AbstractPacketDemultiplexer<net.minidev.json.JSONObject> {
	/**
	 * @param numThreads
	 */
	public PaxosPacketDemultiplexerJSONSmart(int numThreads) {
		super(numThreads);
	}

	/**
	 * @param jsonS
	 * @param unstringer
	 * @return Parsed PaxosPacket.
	 * @throws JSONException
	 */
	public static PaxosPacket toPaxosPacket(net.minidev.json.JSONObject jsonS,
			Stringifiable<?> unstringer) throws JSONException {
		PaxosPacket.PaxosPacketType type = PaxosPacket.PaxosPacketType
				.getPaxosPacketType((Integer) jsonS.get(PaxosPacket.Keys.PT
						.toString()));
		if (type == null)
			fatal(jsonS);

		PaxosPacket paxosPacket = null;
		switch (type) {
		case REQUEST:
			paxosPacket = (new RequestPacket(jsonS));
			break;
		default:
			return PaxosPacketDemultiplexer.toPaxosPacket(toJSONObject(jsonS),
					unstringer);
		}
		assert (paxosPacket != null) : jsonS;
		return paxosPacket;
	}

	/**
	 * @param jsonS
	 * @return JSONObject.
	 * @throws JSONException
	 */
	public static JSONObject toJSONObject(net.minidev.json.JSONObject jsonS)
			throws JSONException {
		JSONObject json = new JSONObject();
		for (String key : jsonS.keySet()) {
			Object value = jsonS.get(key);
			if (value instanceof Collection<?>)
				json.put(key, new JSONArray((Collection<?>) value));
			else
				json.put(key, value);
		}
		return json;
	}

	private static void fatal(Object json) {
		PaxosManager.getLogger().severe(
				PaxosPacketDemultiplexerJSONSmart.class.getSimpleName()
						+ " received " + json);
		throw new RuntimeException(
				"PaxosPacketDemultiplexer recieved unrecognized paxos packet type");
	}

	public abstract boolean handleMessage(net.minidev.json.JSONObject message);

	@Override
	protected Integer getPacketType(net.minidev.json.JSONObject message) {
		return (Integer) message.get(JSONPacket.PACKET_TYPE.toString());
	}

	@Override
	protected net.minidev.json.JSONObject getMessage(String message) {
		return (net.minidev.json.JSONObject) JSONValue.parse(message);
	}

	@Override
	protected net.minidev.json.JSONObject processHeader(String message,
			NIOHeader header) {
		return MessageExtractor.stampAddressIntoJSONObject(header.sndr,
				header.rcvr, MessageExtractor.parseJSON1(message));
	}

	@Override
	public boolean isOrderPreserving(net.minidev.json.JSONObject msg) {
		// only preserve order for REQUEST or PROPOSAL packets
		PaxosPacketType type = PaxosPacket.PaxosPacketType
				.getPaxosPacketType(((Integer) msg.get(PaxosPacket.Keys.PT
						.toString())));
		return (type != null
				&& type.equals(PaxosPacket.PaxosPacketType.REQUEST) || type
					.equals(PaxosPacket.PaxosPacketType.PROPOSAL));
	}
}
