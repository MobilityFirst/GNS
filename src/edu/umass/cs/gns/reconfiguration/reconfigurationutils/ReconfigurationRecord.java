package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;

/**
@author V. Arun
 */
public class ReconfigurationRecord<NodeIDType> extends JSONObject {
	
	public static enum Keys {NAME, EPOCH, RC_STATE, ACTIVES, NEW_ACTIVES, ACKED_START, ACKED_DROP};
	public static enum RCStates {READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP};
	
	private final String name;
	private int epoch=0;
	private RCStates state = RCStates.READY;
	private Set<NodeIDType> actives;
	private Set<NodeIDType> newActives = null;
	private Set<NodeIDType> ackedStart = null; // newActives that have acked startEpoch
	private Set<NodeIDType> ackedDrop = null; // (old) actives that have acked dropEpoch
	
	public ReconfigurationRecord(String name, int epoch, Set<NodeIDType> initialActives) throws JSONException {
		this.name = name;
		this.epoch = epoch; 
		this.actives = initialActives;
		this.put(Keys.NAME.toString(), name);
		this.put(Keys.EPOCH.toString(), epoch);
	}
	
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		json.put(Keys.ACTIVES.toString(), this.actives);
		json.put(Keys.RC_STATE.toString(), this.state.toString());
		json.put(Keys.NEW_ACTIVES.toString(), this.newActives);
		json.put(Keys.ACKED_START.toString(), this.ackedStart);
		json.put(Keys.ACKED_DROP.toString(), this.ackedDrop);
		return json;
	}
	
	public ReconfigurationRecord(JSONObject json) throws JSONException {
		this.name = json.getString(Keys.NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.state = RCStates.valueOf(json.getString(Keys.RC_STATE.toString())); 
		this.actives = toSet(json.getJSONArray(Keys.ACTIVES.toString()));
		this.newActives = toSet(json.getJSONArray(Keys.NEW_ACTIVES.toString()));
		this.ackedStart = toSet(json.getJSONArray(Keys.ACKED_START.toString()));
		this.ackedDrop = toSet(json.getJSONArray(Keys.ACKED_DROP.toString()));
	}
	
	@SuppressWarnings("unchecked")
	private Set<NodeIDType> toSet(JSONArray jsonArray) throws JSONException {
		Set<NodeIDType> set = new HashSet<NodeIDType>();
		for(int i=0; i<jsonArray.length(); i++) {
			set.add((NodeIDType)jsonArray.get(i));
		}
		return set;
	}
	
	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch(JSONException je) {je.printStackTrace();}
		return null;
	}
	
	public int getEpoch() {
		return this.epoch;
	}
	public boolean incrEpoch(int epoch) {
		if(this.epoch==epoch) {
			this.epoch++;
			return true;
		}
		return false;
	}
	public Set<NodeIDType> getActiveReplicas(String name, int epoch) {
		return this.actives;
	}
	public ReconfigurationRecord<NodeIDType> putActiveReplicas(String name, int epoch, Set<NodeIDType> arSet) {
		if(epoch - this.epoch == 1) {
			this.newActives = arSet;
		} 
		return this;
	}
	
	public void setState(RCStates state) {
		this.state = state;
	}
	public RCStates getState() {
		return this.state;
	}
	
	public boolean updateStats(DemandReport<NodeIDType> report) {
		// FIXME: add some actual logic here :)
		return true;
	}
	
	public String getName() {return this.name;}
	
	public static void main(String[] args) {
		try {
			String name = "name1";
			int epoch = 23;
			Integer[] nodes = {2, 43, 54};
			Set<Integer> nodeSet =  new HashSet<Integer>(Arrays.asList(nodes));
			ReconfigurationRecord<Integer> rr1 = new ReconfigurationRecord<Integer>(name, epoch, nodeSet);
			rr1.putActiveReplicas(name, epoch+1, nodeSet);
			System.out.println(rr1.toJSONObject().toString());

			ReconfigurationRecord<Integer> rr2 = new ReconfigurationRecord<Integer>(rr1.toJSONObject());
			System.out.println(rr2.toString());
		} catch(JSONException je) {je.printStackTrace();}
	}
}
