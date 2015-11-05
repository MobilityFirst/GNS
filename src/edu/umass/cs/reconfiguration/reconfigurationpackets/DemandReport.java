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

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
public class DemandReport<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> implements
		ReplicableRequest {
	private enum Keys {
		STATS
	};

	private final JSONObject stats;

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param stats
	 */
	public DemandReport(NodeIDType initiator, String name, int epochNumber,
			JSONObject stats) {
		super(initiator, ReconfigurationPacket.PacketType.DEMAND_REPORT, name,
				epochNumber);
		this.stats = stats;
	}

	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param demand
	 */
	public DemandReport(NodeIDType initiator, String name, int epochNumber,
			AbstractDemandProfile demand) {
		super(initiator, ReconfigurationPacket.PacketType.DEMAND_REPORT, name,
				epochNumber);
		this.stats = demand.getStats();
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public DemandReport(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		this.stats = json.getJSONObject(Keys.STATS.toString());
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.STATS.toString(), this.stats);
		return json;
	}

	/**
	 * @return The demand request statistics returned as a JSON object.
	 */
	public JSONObject getStats() {
		return this.stats;
	}

	@Override
	public IntegerPacketType getRequestType() {
		return ReconfigurationPacket.PacketType.DEMAND_REPORT;
	}

	@Override
	public boolean needsCoordination() {
		return false;
	}

	@Override
	public void setNeedsCoordination(boolean b) {
		// do nothing
	}

	public static void main(String[] args) {
		JSONObject stats = new JSONObject();
		try {
			Util.assertAssertionsEnabled();
			stats.put("rate", 0.33);
			stats.put("numRequests", 24);
			stats.put("numTotalRequests", 24);

			DemandReport<Integer> dr = new DemandReport<Integer>(4, "name1", 2,
					stats);
			System.out.println(dr);
			DemandReport<Integer> dr2 = new DemandReport<Integer>(
					dr.toJSONObject(), new StringifiableDefault<Integer>(0));
			System.out.println(dr2);
			assert (dr.toString().length() == dr2.toString().length());
			assert (dr.toString().indexOf("}") == dr2.toString().indexOf("}"));
			assert (dr.toString().equals(dr2.toString())) : dr.toString()
					+ "!=" + dr2.toString();
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}
}
