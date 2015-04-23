package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.StringifiableDefault;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
public class ReconfigurationRecord<NodeIDType> extends JSONObject {

	public static enum Keys {
		NAME, EPOCH, RC_STATE, ACTIVES, NEW_ACTIVES, ACKED_START, ACKED_DROP, PRIMARY
	};

	public static enum RCStates {
		READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP, WAIT_ACK_DELETE
	};

	private final String name;
	private int epoch = 0;
	private Set<NodeIDType> actives = null; // current epoch
	private RCStates state = RCStates.READY;

	private Set<NodeIDType> newActives = null; // next epoch during epoch change

	public ReconfigurationRecord(String name, int epoch,
			Set<NodeIDType> newActives) {
		this.name = name;
		this.epoch = epoch;
		this.newActives = newActives;
	}
	public ReconfigurationRecord(String name, int epoch, Set<NodeIDType> actives,
			Set<NodeIDType> newActives) {
		this.name = name;
		this.epoch = epoch;
		this.actives = actives;
		this.newActives = newActives;
	}

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		if(this.actives!=null) json.put(Keys.ACTIVES.toString(), this.actives);
		json.put(Keys.RC_STATE.toString(), this.state.toString());
		json.put(Keys.NEW_ACTIVES.toString(), this.newActives);
		//if (this.primary != null) json.put(Keys.PRIMARY.toString(), this.primary);
		return json;
	}

	public ReconfigurationRecord(JSONObject json,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		this.name = json.getString(Keys.NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.state = RCStates.valueOf(json.getString(Keys.RC_STATE.toString()));
		this.actives = json.has(Keys.ACTIVES.toString()) ? toSet(json.getJSONArray(Keys.ACTIVES.toString())) : null;
		this.newActives = toSet(json.getJSONArray(Keys.NEW_ACTIVES.toString()));
		//this.primary = (json.has(Keys.PRIMARY.toString()) ? unstringer.valueOf(json.getString(Keys.PRIMARY.toString())) : null);
	}

	@SuppressWarnings("unchecked")
	private Set<NodeIDType> toSet(JSONArray jsonArray) throws JSONException {
		Set<NodeIDType> set = new HashSet<NodeIDType>();
		for (int i = 0; i < jsonArray.length(); i++) {
			set.add((NodeIDType) jsonArray.get(i));
		}
		return set;
	}

	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return null;
	}

	public int getEpoch() {
		return this.epoch;
	}

	public boolean setEpoch(int epoch) {
		boolean changed = this.epoch == epoch;
		this.epoch = epoch;
		return changed;
	}

	public boolean incrEpoch(int epoch) {
		if (this.epoch == epoch) {
			this.epoch++;
			return true;
		}
		return false;
	}

	public Set<NodeIDType> getActiveReplicas(String name, int epoch) {
		if ((this.name.equals(name) && this.epoch == epoch))
			return this.actives;
		else
			assert (false) : name + ":" + epoch + " != " + this.name + ":"
					+ this.epoch;
		return null;
	}

	public Set<NodeIDType> getActiveReplicas() {
		return this.actives;
	}

	public ReconfigurationRecord<NodeIDType> putActiveReplicas(String name,
			int epoch, Set<NodeIDType> arSet) {
		if (epoch - this.epoch == 1) {
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

	public void setNewActives(Set<NodeIDType> newActives) {
		this.newActives = newActives;
	}

	public void setActivesToNewActives() {
		this.actives = this.newActives;
	}

	public Set<NodeIDType> getNewActives() {
		return this.newActives;
	}

	public boolean updateStats(DemandReport<NodeIDType> report) {
		// FIXME: add some actual logic here :)
		return true;
	}

	public void setState(String name, int epoch, RCStates state) {
		assert (this.name.equals(name));
		assert (this.epoch == epoch || state.equals(RCStates.WAIT_ACK_START) || state
				.equals(RCStates.READY)) : this.epoch + "!=" + epoch;
		this.epoch = epoch;
		this.state = state;
	}
        
        public void setState(String name, int epoch, RCStates state, Set<NodeIDType> newActives) {
          this.setState(name, epoch, state); 
          this.newActives = newActives; 
        }

	public String getName() {
		return this.name;
	}
	
	/*
	public NodeIDType getPrimary() {
		return this.primary;
	}
	public void setPrimary(NodeIDType id) {
		this.primary = id;
	}
	*/
	
	private static boolean setEquals(Set<?> set1, Set<?> set2) {
		return (set1==null && set2==null) || (set1!=null && set2 !=null && set1.equals(set2));
	}
	
	// FIXME: Ignores the "primary" field that needs to be deprecated
	public boolean equals(ReconfigurationRecord<NodeIDType> record) {
		return this.name.equals(record.getName()) && this.state.equals(record.getState()) &&
				this.epoch==record.getEpoch() && setEquals(this.actives, record.getActiveReplicas()) &&
				this.newActives.equals(record.getNewActives());
	}

	public static void main(String[] args) {
		try {
			Util.assertAssertionsEnabled();
			String name = "name1";
			int epoch = 23;
			Integer[] nodes = { 2, 43, 54 };
			Set<Integer> nodeSet = new HashSet<Integer>(Arrays.asList(nodes));
			ReconfigurationRecord<Integer> rr1 = new ReconfigurationRecord<Integer>(
					name, epoch, nodeSet);
			rr1.putActiveReplicas(name, epoch + 1, nodeSet);
			System.out.println(rr1.toJSONObject().toString());

			ReconfigurationRecord<Integer> rr2 = new ReconfigurationRecord<Integer>(
					rr1.toJSONObject(), new StringifiableDefault<Integer>(0));
			System.out.println(rr2.toString());
			assert(rr1.equals(rr2));
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}
}
