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
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;


/**
@author V. Arun
 * @param <NodeIDType> 
 */
public abstract class BasicReconfigurationPacket<NodeIDType> extends ReconfigurationPacket<NodeIDType> implements Request  {

	protected enum Keys {SERVICE_NAME, EPOCH_NUMBER, IS_COORDINATION};

	protected final String serviceName;
	protected final int epochNumber;

	/**
	 * @param initiator
	 * @param t
	 * @param name
	 * @param epochNumber
	 */
	public BasicReconfigurationPacket(NodeIDType initiator, PacketType t, String name, int epochNumber) {
		super(initiator);
		this.setType(t);
		this.serviceName = name;
		this.epochNumber = epochNumber;
	}
	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public BasicReconfigurationPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.serviceName = json.getString(Keys.SERVICE_NAME.toString());
		this.epochNumber = json.getInt(Keys.EPOCH_NUMBER.toString());
	}
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SERVICE_NAME.toString(), this.serviceName);
		json.put(Keys.EPOCH_NUMBER.toString(), this.epochNumber);
		return json;
	}

	public String getServiceName() {
		return this.serviceName;
	}
	/**
	 * @return Epoch number.
	 */
	public int getEpochNumber() {
		return this.epochNumber;
	}
	/**
	 * @return A pretty-print summary.
	 */
	public String getSummary() {
		return getType() + ":"+getServiceName() +":"+getEpochNumber();
	}
	
	public IntegerPacketType getRequestType() {
		return this.getType();
	}
	

	static void main(String[] args) {
		class BRP extends BasicReconfigurationPacket<Integer> {
			BRP(Integer initiator, PacketType t, String name, int epochNumber) {
				super(initiator, t, name, epochNumber);
			}
			BRP(JSONObject json) throws JSONException {
				super(json, new StringifiableDefault<Integer>(0));
			}
		}
		BRP brc = new BRP(3, ReconfigurationPacket.PacketType.DEMAND_REPORT, "name1", 4);
		System.out.println(brc);
		try {
			System.out.println(new BRP(brc.toJSONObject()));
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
