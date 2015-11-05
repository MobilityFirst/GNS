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
package edu.umass.cs.gigapaxos.paxospackets;

import java.net.InetSocketAddress;

import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * 
 * @author arun
 * @param <NodeIDType>
 * 
 *            This "failure detection" packet is really a keep-alive, i.e., its
 *            purpose is to tell the recipient that the sender is up.
 */

@SuppressWarnings("javadoc")
public class FailureDetectionPacket<NodeIDType> extends PaxosPacket {

	private static enum Keys {
		SNDR, RCVR, MODE, SADDR
	};

	/**
	 * Node ID of sender sending this keep-alive packet.
	 */
	public final NodeIDType senderNodeID;
	/**
	 * Destination to which the keep-alive being sent.
	 */
	private final NodeIDType responderNodeID;
	/**
	 * A status flag that is currently not used for anything.
	 */
	private final boolean status;
	
	/** Need this if sender's address is different from that in node config.
	 */
	private InetSocketAddress saddr=null;

	public FailureDetectionPacket(NodeIDType senderNodeID,
			NodeIDType responderNodeID, boolean status) {
		super((PaxosPacket) null);
		this.senderNodeID = senderNodeID;
		this.responderNodeID = responderNodeID;
		this.packetType = PaxosPacketType.FAILURE_DETECT;
		this.status = status;
	}

	public FailureDetectionPacket(JSONObject json,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json);
		this.senderNodeID = unstringer.valueOf(json.getString(Keys.SNDR
				.toString()));
		this.responderNodeID = unstringer.valueOf(json.getString(Keys.RCVR
				.toString()));
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.FAILURE_DETECT);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.status = json.getBoolean(Keys.MODE.toString());
		this.saddr = MessageNIOTransport.getSenderAddress(json);
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.MODE.toString(), status);
		json.put(Keys.SNDR.toString(), senderNodeID);
		json.put(Keys.RCVR.toString(), responderNodeID);
		json.putOpt(Keys.SADDR.toString(), this.saddr);
		return json;
	}
	
	public InetSocketAddress getSender() {
		return this.saddr;
	}

	@Override
	protected String getSummaryString() {
		return "";
	}

}
