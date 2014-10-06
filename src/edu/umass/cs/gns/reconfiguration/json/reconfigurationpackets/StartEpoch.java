package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import java.util.TreeSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.util.Util;

/**
@author V. Arun
 */
public class StartEpoch<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	private enum Keys {PREV_EPOCH_GROUP, CUR_EPOCH_GROUP};

	private final Set<NodeIDType> prevEpochGroup;
	private final Set<NodeIDType> curEpochGroup;

	public StartEpoch(NodeIDType initiator, String serviceName, int epochNumber, Set<NodeIDType> curNodes, Set<NodeIDType> prevNodes) {
		super(initiator, ReconfigurationPacket.PacketType.START_EPOCH, serviceName, epochNumber);
		this.prevEpochGroup = prevNodes;
		this.curEpochGroup = curNodes;
	}

	public StartEpoch(JSONObject json) throws JSONException {
		super(json);
		JSONArray jsonArray = json.getJSONArray(Keys.PREV_EPOCH_GROUP.toString());
		this.prevEpochGroup = new TreeSet<NodeIDType>();
		for(int i=0; i<jsonArray.length(); i++) {
			this.prevEpochGroup.add((NodeIDType)jsonArray.get(i));
		}
		jsonArray = json.getJSONArray(Keys.CUR_EPOCH_GROUP.toString());
		this.curEpochGroup = new TreeSet<NodeIDType>();
		for(int i=0; i<jsonArray.length(); i++) {
			this.curEpochGroup.add((NodeIDType)jsonArray.get(i));
		}
	}
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		
		JSONArray prevGroup = new JSONArray();
		if(this.prevEpochGroup!=null) {
			int i=0; for(NodeIDType s : this.prevEpochGroup) {
				prevGroup.put(i++, s);
			}
		}
		json.put(Keys.PREV_EPOCH_GROUP.toString(), prevGroup);
		
		JSONArray curGroup = new JSONArray();
		int i=0; for(NodeIDType s : this.curEpochGroup) {
			curGroup.put(i++, s);
		}
		json.put(Keys.CUR_EPOCH_GROUP.toString(), curGroup);

		return json;
	}

	public Set<NodeIDType> getCurEpochGroup() {
		return this.curEpochGroup;
	}
	public Set<NodeIDType> getPrevEpochGroup() {
		return this.prevEpochGroup;
	}

	public static void main(String[] args) {
		int[] group = {3, 45, 6, 19};
		StartEpoch<Integer> se = new StartEpoch<Integer>(4, "name1", 2, Util.arrayToIntSet(group), Util.arrayToIntSet(group));
		try {
			System.out.println(se);
			StartEpoch<Integer> se2 = new StartEpoch<Integer>(se.toJSONObject());
			System.out.println(se2);
			assert(se.toString().length()==se2.toString().length());
			assert(se.toString().indexOf("}") == se2.toString().indexOf("}"));
			assert(se.toString().equals(se2.toString())) : se.toString() + "!=" + se2.toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
