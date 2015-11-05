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
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.reconfigurationpackets.DemandReport;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType>
 */
public class ReconfigurationRecord<NodeIDType> extends JSONObject {

	protected static enum Keys {
		NAME, EPOCH, RC_STATE, ACTIVES, NEW_ACTIVES, MERGED, // already merged
		RC_NODE, RC_EPOCH, RC_EPOCH_MAP, STOPPED, MERGE_TASKS, // merge todos
		DELETE_TIME, NUM_UNCLEAN
	};

	/**
	 * RC record states.
	 */
	public static enum RCStates {
		/**
		 * The default state and one of only two states from which
		 * reconfiguration can be initiated, the other being READY_READY.
		 */
		READY,

		/**
		 * Waiting for AckStopEpoch, typically for the epoch that was READY just
		 * before this state transition.
		 */
		WAIT_ACK_STOP,

		/**
		 * Waiting for AckStartEpoch, for the incremented, current epoch that is
		 * at least one higher than the epoch just before the transition.
		 * Typically, an RC record transitions from WAIT_ACK_STOP(n) to
		 * WAIT_ACK_START(n+1), where n and n+1 are epoch numbers. This state is
		 * currently not used as we directly skip from WAIT_ACK_STOP to READY.
		 */
		WAIT_ACK_START,

		/**
		 * Waiting for AckDropEpoch for the previous epoch. This state is not
		 * currently used explicitly because we transition directly from
		 * WAIT_ACK_START to READY because reconfiguration is technically
		 * complete when a majority of AckStartEpochs have been received, so we
		 * simply transition to READY while issuing DropEpoch requests for the
		 * previous epoch lazily. Dropping state for the previous epoch is
		 * needed only for garbage collection and does not impact safety.
		 */
		READY_READY,

		/**
		 * This is a pending delete state, i.e., we expect a deletion to occur
		 * in the near future, but we can not delete it yet because
		 * WaitAckDropEpoch may not have yet completed.
		 */
		WAIT_DELETE
	};

	private final String name;
	private int epoch = 0;
	private Set<NodeIDType> actives = null; // current epoch
	private RCStates state = RCStates.READY;
	private Set<NodeIDType> newActives = null; // next epoch during epoch change

	private Set<String> merged = null; // for at most once semantics
	private Set<String> toMerge = null;

	// used only in NC records to maintain RC epoch numbers consistently
	private HashMap<NodeIDType, Integer> rcEpochs = new HashMap<NodeIDType, Integer>();

	// to let deletes wait for MAX_FINAL_STATE_AGE by default
	private Long deleteTime = null;
	// optimization to track whether quick final-deletion is safe
	private int numPossiblyUncleanReconfigurations = 0;
	
	private String rcGroupName = null;

	/**
	 * @param name
	 * @param epoch
	 * @param newActives
	 */
	public ReconfigurationRecord(String name, int epoch,
			Set<NodeIDType> newActives) {
		this(name, epoch, null, newActives);
	}

	/**
	 * @param name
	 * @param epoch
	 * @param actives
	 * @param newActives
	 */
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

	/**
	 * @return JSON serialization of this object.
	 * @throws JSONException
	 */
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.NAME.toString(), this.name);
		json.put(Keys.EPOCH.toString(), this.epoch);
		if (this.actives != null)
			json.put(Keys.ACTIVES.toString(), this.actives);
		json.put(Keys.RC_STATE.toString(), this.state.toString());
		json.put(Keys.NEW_ACTIVES.toString(), this.newActives);
		json.putOpt(Keys.MERGED.toString(), this.merged);
		json.putOpt(Keys.MERGE_TASKS.toString(), this.toMerge);
		json.putOpt(Keys.RC_EPOCH_MAP.toString(), this.mapToJSONArray(rcEpochs));
		json.putOpt(Keys.DELETE_TIME.toString(), this.deleteTime);
		json.put(Keys.NUM_UNCLEAN.toString(),
				this.numPossiblyUncleanReconfigurations);
		return json;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
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
		this.toMerge = json.has(Keys.MERGE_TASKS.toString()) ? toStringSet(json
				.getJSONArray(Keys.MERGE_TASKS.toString())) : null;
		this.rcEpochs = this.jsonArrayToMap(
				json.getJSONArray(Keys.RC_EPOCH_MAP.toString()), unstringer);
		this.deleteTime = (json.has(Keys.DELETE_TIME.toString()) ? json
				.getLong(Keys.DELETE_TIME.toString()) : null);
		this.numPossiblyUncleanReconfigurations = json.getInt(Keys.NUM_UNCLEAN
				.toString());
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

	/**
	 * @return Current epoch number.
	 */
	public int getEpoch() {
		return this.epoch;
	}

	/**
	 * @param epoch
	 * @return Whether epoch number changed.
	 */
	public boolean setEpoch(int epoch) {
		boolean changed = this.epoch == epoch;
		this.epoch = epoch;
		return changed;
	}

	/**
	 * @param epoch
	 * @return Whether epoch number was incremented.
	 */
	public boolean incrEpoch(int epoch) {
		if (this.epoch == epoch) {
			this.epoch++;
			return true;
		}
		return false;
	}

	/**
	 * @param name
	 * @param epoch
	 * @return Set of active replicas for {@code name:epoch}
	 */
	public Set<NodeIDType> getActiveReplicas(String name, int epoch) {
		if ((this.name.equals(name) && this.epoch == epoch))
			return this.getActiveReplicas();
		else // should never get called otherwise
			assert (false) : name + ":" + epoch + " != " + this.name + ":"
					+ this.epoch;
		return null;
	}

	/**
	 * @return Set of current active replicas. Will return null if this record
	 *         is pending a delete, will return newActives if it is awaiting the
	 *         start of the next epoch, and the current actives otherwise.
	 */
	public Set<NodeIDType> getActiveReplicas() {
		Set<NodeIDType> activeReplicas = !this.getState().equals(
				RCStates.WAIT_DELETE) ? this.actives : null;
		return activeReplicas;// !=null ? activeReplicas : this.newActives;
	}

	/**
	 * @param name
	 * @param epoch
	 * @param arSet
	 * @return Returns this ReconfigurationRecord after modification.
	 */
	public ReconfigurationRecord<NodeIDType> putActiveReplicas(String name,
			int epoch, Set<NodeIDType> arSet) {
		if (epoch - this.epoch == 1) {
			this.newActives = arSet;
		}
		return this;
	}

	/**
	 * @return RCStates.
	 */
	public RCStates getState() {
		return this.state;
	}

	/**
	 * @return True if ready for the next epoch (but may not have dropped
	 *         previous epoch final state yet).
	 */
	public boolean isReady() {
		return this.state.equals(RCStates.READY)
				|| this.state.equals(RCStates.READY_READY);
	}

	/**
	 * @return True if the new active replica group is ready and aggressive
	 *         reconfigurations are allowed or the previous epoch final state
	 *         has been completely dropped (a stronger condition), and in either
	 *         case, merges are all done.
	 */
	public boolean isReconfigurationReady() {
		return
		// unclean ready
		((this.state.equals(RCStates.READY) && ReconfigurationConfig
				.aggressiveReconfigurationsAllowed())
		// clean ready
				|| this.state.equals(RCStates.READY_READY))
				// only relevant for RC groups
				&& this.areMergesAllDone();
	}

	/**
	 * 
	 */
	public void setActivesToNewActives() {
		this.actives = this.newActives;
	}

	/**
	 * @return Set of new active replicas.
	 */
	public Set<NodeIDType> getNewActives() {
		return this.newActives;
	}

	/**
	 * @param s
	 * @return Whether the string being added was already present,.
	 */
	public boolean insertMerged(String s) {
		if (this.merged == null)
			this.merged = new HashSet<String>();
		return this.merged.add(s);
	}

	/**
	 * @param s
	 * @return Whether the string being added was already present.
	 */
	public boolean addToMerge(String s) {
		if (this.toMerge == null)
			this.toMerge = new HashSet<String>();
		return this.toMerge.add(s);
	}

	/**
	 * @return True if toMerge has not yet been initialized.
	 */
	public boolean isToMergeNull() {
		return this.toMerge == null;
	}

	/**
	 * 
	 */
	public void clearMerged() {
		if (this.merged != null)
			this.merged.clear();
		if (this.toMerge != null)
			this.toMerge.clear();
	}

	/**
	 * @param mergee
	 * @return If {@code mergee} was removed.
	 */
	public boolean clearMerged(String mergee) {
		if (this.merged != null)
			return this.merged.remove(mergee);
		return false;
	}

	/**
	 * @param s
	 * @return True if contains {@code s}.
	 */
	public boolean hasBeenMerged(String s) {
		return this.merged != null && this.merged.contains(s);
	}

	/**
	 * @param mergees
	 * @return True if contains all {@code mergees}.
	 */
	public boolean hasBeenMerged(Set<String> mergees) {
		return this.merged != null && this.merged.contains(mergees);
	}

	/**
	 * @return True if all merges are done.
	 */
	public boolean areMergesAllDone() {
		if (this.toMerge == null)
			return true;
		return this.toMerge.equals(this.merged);
	}

	/**
	 * @param s
	 * @return True is toMerge contains s.
	 */
	public boolean toMergeContains(String s) {
		return this.toMerge != null && this.toMerge.contains(s);
	}

	/**
	 * @return True if this record is pending a delete.
	 */
	public boolean isDeletePending() {
		return this.state.equals(RCStates.WAIT_DELETE);
	}

	/**
	 * @param report
	 * @return Whether stats were updated successfully,
	 */
	public boolean updateStats(DemandReport<NodeIDType> report) {
		// FIXME: add some actual logic here :)
		return true;
	}

	/**
	 * @return True if reset to 0.
	 */
	public boolean setDropPreviousEpochCompleted() {
		return --this.numPossiblyUncleanReconfigurations == 0;
	}

	/**
	 * @param name
	 * @param epoch
	 * @param state
	 * @return {@code this}
	 */
	public ReconfigurationRecord<NodeIDType> setState(String name, int epoch, RCStates state) {
		assert (this.name.equals(name) && (this.epoch == epoch
				|| state.equals(RCStates.READY) // common case
				|| state.equals(RCStates.READY_READY) // creation
		|| state.equals(RCStates.WAIT_DELETE))) : this.epoch + "!=" + epoch;

		/*
		 * any lower epoch state to READY -> incr
		 * one lower epoch READY_READY to READY -> decr
		 */
		// !READY to READY => unclean
		if (state.equals(RCStates.READY))
			this.numPossiblyUncleanReconfigurations += (epoch - this.epoch);
		// READY to READY_READY => clean
		else if (state.equals(RCStates.READY_READY)
				&& ((epoch == this.epoch && !this.state
						.equals(RCStates.READY_READY)) || (epoch - this.epoch == 1 && this.state
						.equals(RCStates.READY_READY))))
			this.numPossiblyUncleanReconfigurations--;

		if (this.numPossiblyUncleanReconfigurations < 0)
			this.numPossiblyUncleanReconfigurations = 0;

		this.epoch = epoch;
		this.state = state;

		if (state.equals(RCStates.WAIT_DELETE))
			this.deleteTime = System.currentTimeMillis();
		
		return this;
	}

	/**
	 * @param name
	 * @param epoch
	 * @param state
	 * @param mergees
	 */
	public void setStateMerge(String name, int epoch, RCStates state,
			Set<String> mergees) {
		this.setState(name, epoch, state);
		if (mergees != null)
			for (String mergee : mergees)
				this.addToMerge(mergee);
	}

	/**
	 * @return True if it can be safely deleted.
	 */
	public boolean isDeletable() {
		// must be WAIT_DELETE state and
		return this.state.equals(RCStates.WAIT_DELETE) && (
		// (all clean reconfigurations since creation
				this.getUnclean() == 0
				// or has spent long enough time in WAIT_DELETE state)
				|| System.currentTimeMillis() - this.deleteTime > ReconfigurationConfig
						.getMaxFinalStateAge());
	}

	/**
	 * @return Number of unclean reconfigurations.
	 */
	public int getUnclean() {
		return this.numPossiblyUncleanReconfigurations;
	}

	/**
	 * @param name
	 * @param epoch
	 * @param state
	 * @param newActives
	 */
	public void setState(String name, int epoch, RCStates state,
			Set<NodeIDType> newActives) {
		this.setState(name, epoch, state);
		this.newActives = newActives;
	}

	/**
	 * @return Nale of replica group.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * To get the RC epoch upon a node config change completion. Used only when
	 * reconfiguring reconfigurators.
	 * 
	 * @param rcGroupName
	 * @return The epoch number for the reconfigurator group name.
	 */
	public Integer getRCEpoch(String rcGroupName) {
		for (NodeIDType node : this.rcEpochs.keySet()) {
			if (node.toString().equals(rcGroupName))
				return this.rcEpochs.get(node);
		}
		throw new RuntimeException("Unable to obtain RC epoch for "
				+ rcGroupName);
	}

	/**
	 * @param affectedRCs
	 * @param addNodes
	 * @param deleteNodes
	 */
	public void setRCEpochs(Set<NodeIDType> affectedRCs,
			Set<NodeIDType> addNodes, Set<NodeIDType> deleteNodes) {
		for (NodeIDType node : affectedRCs) {
			if (this.rcEpochs.containsKey(node)) {
				if (!deleteNodes.contains(node))
					this.rcEpochs.put(node, this.rcEpochs.get(node) + 1);
				// else this.rcEpochs.remove(node);
			} else if (addNodes.contains(node)) {
				this.rcEpochs.put(node, 0);
			} else
				assert (false);
		}
	}

	/**
	 * Needed to trim the rcEpochs map when reconfigurator nodes get deleted.
	 */
	public void trimRCEpochs() {
		if (!this.getName().equals(
				AbstractReconfiguratorDB.RecordNames.NODE_CONFIG.toString()))
			return;
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

	/**
	 * @return The time when the state was changed to WAIT_DELETE.
	 */
	public long getDeleteTime() {
		return this.deleteTime;
	}
	
	/**
	 * @param groupName
	 * @return {@code this}
	 */
	public ReconfigurationRecord<NodeIDType> setRCGroupName(String groupName) {
		this.rcGroupName = groupName;
		return this;
	}
	/**
	 * @return RC group name to which this record currently belongs.
	 */
	public String getRCGroupName() {
		return this.rcGroupName;
	}

	/**
	 * @return For instrumentation purposes.
	 */
	public String getSummary() {
		return this.getName()
				+ ":"
				+ this.getEpoch()
				+ ":"
				+ this.getState()
				+ (!this.areMergesAllDone() ? ":!merged[" + this.merged + "!="
						+ this.toMerge + "]"
						: !this.isToMergeNull() ? " merged[" + this.merged
								+ "==" + this.toMerge + "]" : "");
	}

	static void main(String[] args) {
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
