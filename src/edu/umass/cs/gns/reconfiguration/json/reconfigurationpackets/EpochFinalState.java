package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class EpochFinalState<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {
	
	private enum Keys {EPOCH_FINAL_STATE};
	
	private final String state;
	
	public EpochFinalState(NodeIDType initiator, String name, int epochNumber, String state) {
		super(initiator, ReconfigurationPacket.PacketType.EPOCH_FINAL_STATE, name, epochNumber);
		this.state = state;
	}
	public EpochFinalState(JSONObject json) throws JSONException {
		super(json);
		this.state = (json.has(Keys.EPOCH_FINAL_STATE.toString()) ? json.getString(Keys.EPOCH_FINAL_STATE.toString()) : null);
	}
	public String getState() {return this.state;}
	
	@Override
	public JSONObject toJSONObjectImpl() throws JSONException  {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.EPOCH_FINAL_STATE.toString(), this.state);
		return json;
	}
	
	public static void main(String[] args) {
		int[] group = {3, 45, 6, 19};
		EpochFinalState<Integer> obj1 = new EpochFinalState<Integer>(4, "name1", 2, "sample_state");
		try {
			System.out.println(obj1);
			EpochFinalState<Integer> obj2 = new EpochFinalState<Integer>(obj1.toJSONObject());
			System.out.println(obj2);
			assert(obj1.toString().length()==obj2.toString().length());
			assert(obj1.toString().indexOf("}") == obj2.toString().indexOf("}"));
			assert(obj1.toString().equals(obj2.toString())) : obj1.toString() + "!=" + obj2.toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
