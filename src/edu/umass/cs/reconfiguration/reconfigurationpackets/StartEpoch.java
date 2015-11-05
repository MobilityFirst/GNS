/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.StringifiableDefault;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            FIXME: This class is increasingly under risk of becoming the
 *            kitchen sink for every piece of information needed anywhere in the
 *            reconfiguration protocol sequence. A better design may be to carry
 *            the causal reconfiguration packet (create, delete, reconfigureRC,
 *            etc.) here instead of carrying all fields in all startEpoch
 *            packets.
 */
public class StartEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> {

	protected static enum Keys {
		PREV_EPOCH_GROUP, CUR_EPOCH_GROUP, CREATOR, RECEIVER, FORWARDER, INITIAL_STATE,
		//
		PREV_GROUP_NAME, NEWLY_ADDED_NODES, NODE_ID, SOCKET_ADDRESS, PREV_EPOCH,
		//
		IS_MERGE, INIT_TIME, MERGEES, FIRST_PREV_EPOCH_CANDIDATE, NAME_STATE_ARRAY
	};

	/**
	 * Group members in the epoch just before the one being started.
	 */
	public final Set<NodeIDType> prevEpochGroup;
	/**
	 * Group members in the current epoch being started.
	 */
	public final Set<NodeIDType> curEpochGroup;
	/**
	 * Initial state of epoch being started.
	 */
	public final String initialState;

	/**
	 * Creation time that remains unchanged across network transmissions.
	 */
	private final long initTime;

	/**
	 * Sender address.
	 */
	public final InetSocketAddress creator; // for creation (or first) epoch

	/**
	 * Receiver address.
	 */
	public final InetSocketAddress receiver; // address of receiving server

	/**
	 * For supporting RC group merge or split operations wherein the previous
	 * epoch group may have a different name.
	 */
	public final String prevGroupName; // for merge or split group operations
	/**
	 * For supporting RC group merge or split operations wherein the previous
	 * epoch group may have a different name and epoch number.
	 */
	public final int prevEpoch;

	/**
	 * Whether the corresponding reconfiguration is a merge (used in RC group
	 * reconfiguration when RC nodes are deleted).
	 */
	public final boolean isMerge;

	/**
	 * Socket address map for new RC nodes being added.
	 */
	public final Map<NodeIDType, InetSocketAddress> newlyAddedNodes;

	private final InetSocketAddress forwarder;

	/**
	 * The list of deleted nodes that must be merged with this node. Nonempty
	 * only if prevGroupName==curGroupName and this is an RC group record and
	 * there are nodes to be merged with this node.
	 */
	public final Set<String> mergees;

	private NodeIDType firstPrevEpochCandidate = null;

	private final Map<String, String> nameStates;

	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param curNodes
	 * @param prevNodes
	 * @param mergees
	 */
	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, Set<String> mergees) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes, null,
				false, -1, null, null, null, null, null, null, mergees);
	}

	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param curNodes
	 * @param prevNodes
	 * @param prevGroupName
	 * @param isMerge
	 * @param prevEpoch
	 */
	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, String prevGroupName, boolean isMerge,
			int prevEpoch) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes,
				prevGroupName, isMerge, prevEpoch, null, null, null, null,
				null, null);
	}

	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param curNodes
	 * @param prevNodes
	 * @param creator
	 * @param receiver
	 * @param forwarder
	 * @param initialState
	 * @param nameStates
	 * @param newlyAddedNodes
	 */
	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, InetSocketAddress creator,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes, null,
				false, -1, creator, receiver, forwarder, initialState,
				nameStates, newlyAddedNodes);
	}

	/**
	 * Just to reset final field {@code initialState}.
	 * 
	 * @param startEpoch
	 * @param initialState
	 */
	public StartEpoch(StartEpoch<NodeIDType> startEpoch, String initialState) {
		this(startEpoch.getInitiator(), startEpoch.getServiceName(), startEpoch
				.getEpochNumber(), startEpoch.curEpochGroup,
				startEpoch.prevEpochGroup, startEpoch.prevGroupName,
				startEpoch.isMerge, startEpoch.prevEpoch, startEpoch.creator,
				startEpoch.receiver, startEpoch.forwarder, initialState,
				startEpoch.nameStates, startEpoch.newlyAddedNodes,
				startEpoch.mergees, startEpoch.initTime);
		this.firstPrevEpochCandidate = startEpoch.firstPrevEpochCandidate;
	}

	/**
	 * Just to reset final field {@code initiator}.
	 * 
	 * @param initiator
	 * @param startEpoch
	 */
	public StartEpoch(NodeIDType initiator, StartEpoch<NodeIDType> startEpoch) {
		this(initiator, startEpoch.getServiceName(), startEpoch
				.getEpochNumber(), startEpoch.curEpochGroup,
				startEpoch.prevEpochGroup, startEpoch.prevGroupName,
				startEpoch.isMerge, startEpoch.prevEpoch, startEpoch.creator,
				startEpoch.receiver, startEpoch.forwarder,
				startEpoch.initialState, startEpoch.nameStates,
				startEpoch.newlyAddedNodes, startEpoch.mergees,
				startEpoch.initTime);
		this.firstPrevEpochCandidate = startEpoch.firstPrevEpochCandidate;
	}

	private StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, String prevGroupName, boolean isMerge,
			int prevEpoch, InetSocketAddress creator,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes,
				prevGroupName, isMerge, prevEpoch, creator, receiver,
				forwarder, initialState, nameStates, newlyAddedNodes, null);

	}

	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param curNodes
	 * @param prevNodes
	 * @param prevGroupName
	 * @param isMerge
	 * @param prevEpoch
	 * @param creator
	 * @param initialState
	 * @param newlyAddedNodes
	 */
	private StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, String prevGroupName, boolean isMerge,
			int prevEpoch, InetSocketAddress creator,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes,
			Set<String> mergees) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes,
				prevGroupName, isMerge, prevEpoch, creator, receiver,
				forwarder, initialState, nameStates, newlyAddedNodes, mergees,
				System.currentTimeMillis());
	}

	private StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, String prevGroupName, boolean isMerge,
			int prevEpoch, InetSocketAddress creator,
			InetSocketAddress receiver, InetSocketAddress forwarder,
			String initialState, Map<String, String> nameStates,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes,
			Set<String> mergees, long initTime) {
		super(initiator, ReconfigurationPacket.PacketType.START_EPOCH,
				serviceName, epochNumber);
		this.prevEpochGroup = prevNodes;
		this.curEpochGroup = curNodes;
		this.prevGroupName = prevGroupName;
		this.prevEpoch = prevEpoch;
		this.isMerge = isMerge;
		this.mergees = mergees;
		this.initTime = initTime;

		// ClientReconfigurationPacket fields
		this.creator = creator;
		this.receiver = receiver;
		this.forwarder = forwarder;
		this.initialState = initialState;
		this.nameStates = nameStates;
		
		this.newlyAddedNodes = newlyAddedNodes;
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public StartEpoch(JSONObject json, Stringifiable<NodeIDType> unstringer)
			throws JSONException {
		super(json, unstringer);
		JSONArray jsonArray = json.getJSONArray(Keys.PREV_EPOCH_GROUP
				.toString());
		// prev epoch group
		this.prevEpochGroup = new TreeSet<NodeIDType>();
		for (int i = 0; i < jsonArray.length(); i++) {
			this.prevEpochGroup.add((NodeIDType) unstringer.valueOf(jsonArray
					.get(i).toString()));
		}
		// cur epoch group
		jsonArray = json.getJSONArray(Keys.CUR_EPOCH_GROUP.toString());
		this.curEpochGroup = new TreeSet<NodeIDType>();
		for (int i = 0; i < jsonArray.length(); i++) {
			this.curEpochGroup.add((NodeIDType) unstringer.valueOf(jsonArray
					.get(i).toString()));
		}
		// mergees
		jsonArray = json.getJSONArray(Keys.MERGEES.toString());
		this.mergees = new TreeSet<String>();
		for (int i = 0; i < jsonArray.length(); i++) {
			this.mergees.add(jsonArray.get(i).toString());
		}
		this.creator = (json.has(Keys.CREATOR.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CREATOR
						.toString())) : null);
		this.receiver = (json.has(Keys.RECEIVER.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.RECEIVER
						.toString())) : null);
		this.forwarder = (json.has(Keys.FORWARDER.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.FORWARDER
						.toString())) : null);

		this.prevGroupName = (json.has(Keys.PREV_GROUP_NAME.toString()) ? json
				.getString(Keys.PREV_GROUP_NAME.toString()) : null);
		this.isMerge = json.optBoolean(Keys.IS_MERGE.toString());

		// initialState and possibly nameStates if batched create
		this.initialState = json.optString(Keys.INITIAL_STATE.toString(), null);
		this.nameStates = CreateServiceName.getNameStateMap(json);

		this.newlyAddedNodes = (json.has(Keys.NEWLY_ADDED_NODES.toString()) ? this
				.arrayToMap(
						json.getJSONArray(Keys.NEWLY_ADDED_NODES.toString()),
						unstringer) : null);

		this.prevEpoch = json.optInt(Keys.PREV_EPOCH.toString(), -1);
		this.initTime = json.getLong(Keys.INIT_TIME.toString());
		this.firstPrevEpochCandidate = json.has(Keys.FIRST_PREV_EPOCH_CANDIDATE
				.toString()) ? unstringer.valueOf(json
				.getString(Keys.FIRST_PREV_EPOCH_CANDIDATE.toString())) : null;
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();

		json.put(Keys.PREV_EPOCH_GROUP.toString(),
				this.toJSONArray(prevEpochGroup));
		json.put(Keys.CUR_EPOCH_GROUP.toString(),
				this.toJSONArray(curEpochGroup));
		json.put(Keys.MERGEES.toString(), new JSONArray(this.mergees));

		json.putOpt(Keys.INITIAL_STATE.toString(), initialState);
		json.putOpt(CreateServiceName.Keys.NAME_STATE_ARRAY.toString(),
				CreateServiceName.getNameStateJSONArray(this.nameStates));

		json.put(Keys.CREATOR.toString(), this.creator);
		json.put(Keys.RECEIVER.toString(), this.receiver);
		json.put(Keys.FORWARDER.toString(), this.forwarder);

		json.put(Keys.INIT_TIME.toString(), this.initTime);

		// both prev name and epoch or neither
		if (this.prevGroupName != null) {
			json.put(Keys.PREV_GROUP_NAME.toString(), this.prevGroupName);
			json.put(Keys.PREV_EPOCH.toString(), this.prevEpoch);
			json.put(Keys.IS_MERGE.toString(), this.isMerge);
		}

		if (this.newlyAddedNodes != null)
			json.put(Keys.NEWLY_ADDED_NODES.toString(),
					this.mapToArray(newlyAddedNodes));
		json.putOpt(Keys.FIRST_PREV_EPOCH_CANDIDATE.toString(),
				firstPrevEpochCandidate);
		return json;
	}

	private JSONArray toJSONArray(Set<NodeIDType> set) throws JSONException {
		int i = 0;
		// okay if set is null
		JSONArray jsonArray = new JSONArray(set);
		if (set != null)
			for (NodeIDType member : set) {
				jsonArray.put(i++, member.toString());
			}
		return jsonArray;
	}

	/**
	 * @return Time when the reconfiguration began.
	 */
	public long getInitTime() {
		return this.initTime;
	}

	/**
	 * @return Current epoch group members.
	 */
	public Set<NodeIDType> getCurEpochGroup() {
		return this.curEpochGroup;
	}

	/**
	 * @return Previous epoch group members.
	 */
	public Set<NodeIDType> getPrevEpochGroup() {
		return this.prevEpochGroup;
	}

	/**
	 * @return Set of common members between prev and cur epoch group.
	 */
	public Set<NodeIDType> getCommonMembers() {
		Set<NodeIDType> common = new HashSet<NodeIDType>();
		if (this.hasCurEpochGroup() && this.hasPrevEpochGroup())
			for (NodeIDType curNode : this.curEpochGroup) {
				if (this.prevEpochGroup.contains(curNode))
					common.add(curNode);
			}
		return common;
	}

	/**
	 * @return A single element set containing the initiator node.
	 */
	public Set<NodeIDType> getInitiatorAsSet() {
		HashSet<NodeIDType> set = new HashSet<NodeIDType>();
		set.add(this.getInitiator());
		return set;
	}

	/**
	 * @return Whether the epoch being created has no previous epoch group or it
	 *         is split or merge reconfiguration operation.
	 */
	public boolean isInitEpoch() {
		return (this.noPrevEpochGroup() && this.epochNumber == 0)
				// split or merge operation
				|| (this.prevGroupName != null && (!this.prevGroupName
						.equals(this.getServiceName()) || (this
						.getEpochNumber() == this.getPrevEpochNumber() + 1)));
	}
	
	/**
	 * @return True if this is a batched create request.
	 */
	public boolean isBatchedCreate() {
		return this.isInitEpoch() && this.nameStates!=null && !this.nameStates.isEmpty();
	}

	/**
	 * @return True if current epoch group is empty.
	 */
	public boolean noCurEpochGroup() {
		return this.curEpochGroup == null || this.curEpochGroup.isEmpty();
	}

	/**
	 * @return True if previous epoch group is empty.
	 */
	public boolean noPrevEpochGroup() {
		return this.prevEpochGroup == null || this.prevEpochGroup.isEmpty();
	}

	/**
	 * @return True if previous epoch group is nonempty.
	 */
	public boolean hasPrevEpochGroup() {
		return !this.noPrevEpochGroup();
	}

	/**
	 * @return True if the new epoch group is nonempty.
	 */
	public boolean hasCurEpochGroup() {
		return !this.noCurEpochGroup();
	}

	/**
	 * @return Whether this is a create request, i.e., there is no previous
	 *         epoch group and there is a (non-null) creator.
	 */
	public boolean isCreateRequest() {
		return this.noPrevEpochGroup() && this.creator != null;
	}

	/**
	 * @return Whether this is a create request, i.e., there is no previous
	 *         epoch group and there is a (non-null) creator.
	 */
	public boolean isDeleteRequest() {
		return this.noCurEpochGroup() && this.creator != null;
	}

	/**
	 * @return Initial state.
	 */
	public String getInitialState() {
		return initialState;
	}
	
	/**
	 * @return Name,state map for batched creates.
	 */
	public Map<String,String> getNameStates() {
		return this.nameStates;
	}

	/**
	 * @return Previous epoch group name.
	 */
	public String getPrevGroupName() {
		return this.prevGroupName != null ? this.prevGroupName : this
				.getServiceName();
	}

	/**
	 * @return Previous epoch group number.
	 */
	public int getPrevEpochNumber() {
		return this.prevGroupName != null ? this.prevEpoch : (this
				.getEpochNumber() - 1);
	}

	/**
	 * @return True if the start epoch corresponds to a split or merge
	 *         reconfiguration operation.
	 */
	public boolean isSplitOrMerge() {
		return this.getPrevGroupName() != null
				&& !this.getPrevGroupName().equals(this.getServiceName());
	}

	/**
	 * @return True if it is a split reconfiguration.
	 */
	public boolean isSplit() {
		return isSplitOrMerge() && !isMerge();
	}

	/**
	 * @return True if new RC nodes are being added.
	 */
	public boolean hasNewlyAddedNodes() {
		return this.newlyAddedNodes != null && !this.newlyAddedNodes.isEmpty();
	}

	/**
	 * 
	 * @return True if it is a merge reconfiguration operation.
	 */
	public boolean isMerge() {
		return this.isMerge;
	}

	/**
	 * @return The set of groups to be merged.
	 */
	public Set<String> getMergees() {
		return this.mergees;
	}

	/**
	 * @return True if the set of mergees is nonempty.
	 */
	public boolean hasMergees() {
		return this.mergees != null && !this.mergees.isEmpty();
	}

	/**
	 * @return Set of newly added nodes.
	 */
	public Set<NodeIDType> getNewlyAddedNodes() {
		return this.newlyAddedNodes == null ? new HashSet<NodeIDType>()
				: new HashSet<NodeIDType>(this.newlyAddedNodes.keySet());
	}

	/**
	 * @param node
	 * @return {@code this}
	 */
	public StartEpoch<NodeIDType> setFirstPrevEpochCandidate(NodeIDType node) {
		this.firstPrevEpochCandidate = node;
		return this;
	}

	/**
	 * @return The first candidate to try while requesting previous epoch final
	 *         state.
	 */
	public NodeIDType getFirstPrevEpochCandidate() {
		return this.firstPrevEpochCandidate;
	}

	static void main(String[] args) {
		int[] group = { 3, 45, 6, 19 };
		StartEpoch<Integer> se = new StartEpoch<Integer>(4, "name1", 2,
				Util.arrayToIntSet(group), Util.arrayToIntSet(group), null);
		try {
			System.out.println(se);
			StartEpoch<Integer> se2 = new StartEpoch<Integer>(
					se.toJSONObject(), new StringifiableDefault<Integer>(0));
			System.out.println(se2);
			assert (se.toString().length() == se2.toString().length());
			assert (se.toString().indexOf("}") == se2.toString().indexOf("}"));
			assert (se.toString().equals(se2.toString())) : se.toString()
					+ "!=" + se2.toString();
		} catch (JSONException je) {
			je.printStackTrace();
		}
	}

	// Utility method for newly added nodes
	private Map<NodeIDType, InetSocketAddress> arrayToMap(JSONArray jArray,
			Stringifiable<NodeIDType> unstringer) throws JSONException {
		Map<NodeIDType, InetSocketAddress> map = new HashMap<NodeIDType, InetSocketAddress>();
		for (int i = 0; i < jArray.length(); i++) {
			JSONObject jElement = jArray.getJSONObject(i);
			assert (jElement.has(Keys.NODE_ID.toString()) && jElement
					.has(Keys.SOCKET_ADDRESS.toString()));
			map.put(unstringer.valueOf(jElement.getString(Keys.NODE_ID
					.toString())), Util.getInetSocketAddressFromString(jElement
					.getString(Keys.SOCKET_ADDRESS.toString())));
		}
		return map;
	}

	// Utility method for newly added nodes
	private JSONArray mapToArray(Map<NodeIDType, InetSocketAddress> map)
			throws JSONException {
		JSONArray jArray = new JSONArray();
		for (NodeIDType node : map.keySet()) {
			JSONObject jElement = new JSONObject();
			jElement.put(Keys.NODE_ID.toString(), node.toString());
			jElement.put(Keys.SOCKET_ADDRESS.toString(), map.get(node)
					.toString());
			jArray.put(jElement);
		}
		return jArray;
	}

	/**
	 * @return The forwarder socket address.
	 */
	public InetSocketAddress getForwarder() {
		return this.forwarder;
	}

	/**
	 * @return The socket address on which the initiating request was received.
	 */
	public InetSocketAddress getMyReceiver() {
		return this.receiver;
	}
}
