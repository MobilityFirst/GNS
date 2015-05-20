package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.DemandReport;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.StringifiableDefault;
import edu.umass.cs.gns.util.Util;

/**
 * @author V. Arun
 */
public class ReconfigurationRecord<NodeIDType> extends JSONObject {

	public static enum Keys {
		NAME, EPOCH, RC_STATE, ACTIVES, NEW_ACTIVES, ACKED_START, ACKED_DROP, MERGED, RC_NODE, RC_EPOCH, RC_EPOCH_MAP
	};

	public static enum RCStates {
		READY, WAIT_ACK_STOP, WAIT_ACK_START, WAIT_ACK_DROP, WAIT_ACK_DELETE
	};

	private final String name;
	private int epoch = 0;
	private Set<NodeIDType> actives = null; // current epoch
	private RCStates state = RCStates.READY;
	private Set<NodeIDType> newActives = null; // next epoch during epoch change

	private Set<String> merged = null; // for at most once semantics
	private HashMap<NodeIDType, Integer> rcEpochs = new HashMap<NodeIDType, Integer>();

	public ReconfigurationRecord(String name, int epoch,
			Set<NodeIDType> newActives) {
		this(name, epoch, null, newActives);
	}

	public ReconfigurationRecord(String name, int epoch,
			Set<NodeIDType> actives, Set<NodeIDType> newActives) {
		this.name = name;
		this.epoch = epoch;
		this.actives = actives;
		this.newActives = newActives;
		if (name.equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG
				.toString()))
			for (NodeIDType node : newActives)
				this.rcEpochs.put(node, 0);
	}

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		if (this.actives != null)
			json.put(Keys.ACTIVES.toString(), this.actives);
		json.put(Keys.RC_STATE.toString(), this.state.toString());
		json.put(Keys.NEW_ACTIVES.toString(), this.newActives);
		json.putOpt(Keys.MERGED.toString(), this.merged);
		json.putOpt(Keys.RC_EPOCH_MAP.toString(), this.mapToJSONArray(rcEpochs));
		return json;
	}

	public ReconfigurationRecord(JSONObject json,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		this.name = json.getString(Keys.NAME.toString());
		this.epoch = json.getInt(Keys.EPOCH.toString());
		this.state = RCStates.valueOf(json.getString(Keys.RC_STATE.toString()));
		this.actives = json.has(Keys.ACTIVES.toString()) ? toSet(json
				.getJSONArray(Keys.ACTIVES.toString())) : null;
		this.newActives = toSet(json.getJSONArray(Keys.NEW_ACTIVES.toString()));
		this.merged = json.has(Keys.MERGED.toString()) ? toStringSet(json
				.getJSONArray(Keys.MERGED.toString())) : null;
		this.rcEpochs = this.jsonArrayToMap(
				json.getJSONArray(Keys.RC_EPOCH_MAP.toString()), unstringer);
	}

	@SuppressWarnings("unchecked")
	private Set<NodeIDType> toSet(JSONArray jsonArray) throws JSONException {
		Set<NodeIDType> set = new HashSet<NodeIDType>();
		for (int i = 0; i < jsonArray.length(); i++) {
			set.add((NodeIDType) jsonArray.get(i));
		}
		return set;
	}

	private Set<String> toStringSet(JSONArray jsonArray) throws JSONException {
		Set<String> set = new HashSet<String>();
		for (int i = 0; i < jsonArray.length(); i++) {
			set.add((String) jsonArray.get(i));
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

	public boolean insertMerged(String s) {
		if (this.merged == null)
			this.merged = new HashSet<String>();
		return this.merged.add(s);
	}

	public void clearMerged() {
		if (this.merged != null)
			this.merged.clear();
	}

	public boolean clearMerged(String mergee) {
		if (this.merged != null)
			return this.merged.remove(mergee);
		return false;
	}

	public boolean mergedContains(String s) {
		return this.merged != null && this.merged.contains(s);
	}

	public boolean isMergeAllClear() {
		return this.merged == null || this.merged.isEmpty();
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

	public void setState(String name, int epoch, RCStates state,
			Set<NodeIDType> newActives) {
		this.setState(name, epoch, state);
		this.newActives = newActives;
	}

	public String getName() {
		return this.name;
	}

	/*
	 * FIXME: need to actually make these work correctly. We need to set the RC
	 * epoch upon a node config completion.
	 */
	public Integer getRCEpoch(String rcGroupName) {
		for(NodeIDType node : this.rcEpochs.keySet()) {
			if(node.toString().equals(rcGroupName)) return this.rcEpochs.get(node);
		}
		throw new RuntimeException("Unable to obtain RC epoch for " + rcGroupName);
		// return this.rcEpochs.get(rcGroupName);
	}

	public void setRCEpochs(Set<NodeIDType> affectedRCs,
			Set<NodeIDType> addNodes, Set<NodeIDType> deleteNodes) {
		for (NodeIDType node : affectedRCs) {
			if (this.rcEpochs.containsKey(node)) {
				if (!deleteNodes.contains(node))
					this.rcEpochs.put(node, this.rcEpochs.get(node) + 1);
				//else this.rcEpochs.remove(node);
			}
			else if (addNodes.contains(node)) {
				this.rcEpochs.put(node, 0);
			}
			else
				assert (false);
		}
	}
	
	public void trimRCEpochs() {
		if(!this.getName().equals(AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString())) return;
		for (Iterator<NodeIDType> iterator = this.rcEpochs.keySet().iterator(); iterator
				.hasNext();) {
			if (!this.newActives.contains(iterator.next()))
				iterator.remove();
		}
	}

	private JSONArray mapToJSONArray(Map<NodeIDType, Integer> map)
			throws JSONException {
		JSONArray jArray = new JSONArray();
		for (NodeIDType node : map.keySet()) {
			JSONObject json = new JSONObject();
			json.put(Keys.RC_NODE.toString(), node.toString());
			json.put(Keys.RC_EPOCH.toString(), map.get(node));
			jArray.put(json);
		}
		return jArray;
	}

	private HashMap<NodeIDType, Integer> jsonArrayToMap(JSONArray jArray,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		HashMap<NodeIDType, Integer> map = new HashMap<NodeIDType, Integer>();
		for (int i = 0; i < jArray.length(); i++) {
			JSONObject json = jArray.getJSONObject(i);
			map.put(unstringer.valueOf(json.getString(Keys.RC_NODE.toString())),
					json.getInt(Keys.RC_EPOCH.toString()));
		}
		return map;
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
			assert (rr1.toString().equals(rr2.toString()));

			HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();
			map.put(2, 32);
			map.put(54, 30);
			map.put(43, 33);
			System.out.println(rr1.mapToJSONArray(map));
			System.out.println(rr1.jsonArrayToMap(rr1.mapToJSONArray(map),
					new StringifiableDefault<Integer>(0)));
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}
}
