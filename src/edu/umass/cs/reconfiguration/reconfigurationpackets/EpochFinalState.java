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

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;

/**
@author V. Arun
 * @param <NodeIDType> 
 */
public class EpochFinalState<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {
	
	private enum Keys {EPOCH_FINAL_STATE};
	
	private final String state;
	
	/**
	 * @param initiator
	 * @param name
	 * @param epochNumber
	 * @param state
	 */
	public EpochFinalState(NodeIDType initiator, String name, int epochNumber, String state) {
		super(initiator, ReconfigurationPacket.PacketType.EPOCH_FINAL_STATE, name, epochNumber);
		this.state = state;
	}
	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public EpochFinalState(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.state = (json.has(Keys.EPOCH_FINAL_STATE.toString()) ? json.getString(Keys.EPOCH_FINAL_STATE.toString()) : null);
	}
	/**
	 * @return Epoch final state.
	 */
	public String getState() {return this.state;}
	
	@Override
	public JSONObject toJSONObjectImpl() throws JSONException  {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.EPOCH_FINAL_STATE.toString(), this.state);
		return json;
	}
	
	@Override
	public String getSummary() {
		return super.getSummary() + ":" + this.state;
	}
	
	public static void main(String[] args) {
		EpochFinalState<Integer> obj1 = new EpochFinalState<Integer>(4, "name1", 2, "sample_state");
		try {
			System.out.println(obj1);
			EpochFinalState<Integer> obj2 = new EpochFinalState<Integer>(obj1.toJSONObject(), new StringifiableDefault<Integer>(0));
			System.out.println(obj2);
			assert(obj1.toString().length()==obj2.toString().length());
			assert(obj1.toString().indexOf("}") == obj2.toString().indexOf("}"));
			assert(obj1.toString().equals(obj2.toString())) : obj1.toString() + "!=" + obj2.toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
