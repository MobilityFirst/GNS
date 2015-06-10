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

import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.StringifiableDefault;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * @param <NodeIDType> 
 */
public class StartEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> {

	protected static enum Keys {
		PREV_EPOCH_GROUP, CUR_EPOCH_GROUP, CREATOR, INITIAL_STATE, PREV_GROUP_NAME, NEWLY_ADDED_NODES, NODE_ID, SOCKET_ADDRESS, PREV_EPOCH, IS_MERGE
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
	 * Sender address.
	 */
	public final InetSocketAddress creator; // for creation (or first) epoch

	/**
	 * For supporting RC group merge or split operations wherein the
	 * previous epoch group may have a different name.
	 */
	public final String prevGroupName; // for merge or split group operations
	/**
	 * For supporting RC group merge or split operations wherein the
	 * previous epoch group may have a different name and epoch number.
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

	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param curNodes
	 * @param prevNodes
	 */
	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes, Set<NodeIDType> prevNodes) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes, null,
				false, -1, null, null, null);
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
				prevGroupName, isMerge, prevEpoch, null, null, null);
	}

	/**
	 * @param initiator
	 * @param serviceName
	 * @param epochNumber
	 * @param curNodes
	 * @param prevNodes
	 * @param creator
	 * @param initialState
	 * @param newlyAddedNodes
	 */
	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, InetSocketAddress creator,
			String initialState,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes, null,
				false, -1, creator, initialState, newlyAddedNodes);
	}

	/**
	 * @param startEpoch
	 * @param initialState
	 */
	public StartEpoch(StartEpoch<NodeIDType> startEpoch, String initialState) {
		this(startEpoch.getInitiator(), startEpoch.getServiceName(), startEpoch
				.getEpochNumber(), startEpoch.curEpochGroup,
				startEpoch.prevEpochGroup, startEpoch.prevGroupName,
				startEpoch.isMerge, startEpoch.prevEpoch, startEpoch.creator,
				initialState, startEpoch.newlyAddedNodes);
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
	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, String prevGroupName, boolean isMerge,
			int prevEpoch, InetSocketAddress creator, String initialState,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		super(initiator, ReconfigurationPacket.PacketType.START_EPOCH,
				serviceName, epochNumber);
		this.prevEpochGroup = prevNodes;
		this.curEpochGroup = curNodes;
		this.creator = creator;
		this.prevGroupName = prevGroupName;
		this.initialState = initialState;
		this.newlyAddedNodes = newlyAddedNodes;
		this.prevEpoch = prevEpoch;
		this.isMerge = isMerge;
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
		this.prevEpochGroup = new TreeSet<NodeIDType>();
		for (int i = 0; i < jsonArray.length(); i++) {
			this.prevEpochGroup.add((NodeIDType) unstringer.valueOf(jsonArray
					.get(i).toString()));
		}
		jsonArray = json.getJSONArray(Keys.CUR_EPOCH_GROUP.toString());
		this.curEpochGroup = new TreeSet<NodeIDType>();
		for (int i = 0; i < jsonArray.length(); i++) {
			this.curEpochGroup.add((NodeIDType) unstringer.valueOf(jsonArray
					.get(i).toString()));
		}
		this.creator = (json.has(Keys.CREATOR.toString()) ? Util
				.getInetSocketAddressFromString(json.getString(Keys.CREATOR
						.toString())) : null);
		this.prevGroupName = (json.has(Keys.PREV_GROUP_NAME.toString()) ? json
				.getString(Keys.PREV_GROUP_NAME.toString()) : null);
		this.isMerge = json.optBoolean(Keys.IS_MERGE.toString());
		this.initialState = json.optString(Keys.INITIAL_STATE.toString(), null);

		this.newlyAddedNodes = (json.has(Keys.NEWLY_ADDED_NODES.toString()) ? this
				.arrayToMap(
						json.getJSONArray(Keys.NEWLY_ADDED_NODES.toString()),
						unstringer) : null);
		this.prevEpoch = json.optInt(Keys.PREV_EPOCH.toString(), -1);
	}

	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();

		json.put(Keys.PREV_EPOCH_GROUP.toString(),
				this.toJSONArray(prevEpochGroup));
		json.put(Keys.CUR_EPOCH_GROUP.toString(),
				this.toJSONArray(curEpochGroup));
		if (initialState != null)
			json.put(Keys.INITIAL_STATE.toString(), initialState);

		json.put(Keys.CREATOR.toString(), this.creator);

		// both prev name and epoch or neither
		if (this.prevGroupName != null) {
			json.put(Keys.PREV_GROUP_NAME.toString(), this.prevGroupName);
			json.put(Keys.PREV_EPOCH.toString(), this.prevEpoch);
			json.put(Keys.IS_MERGE.toString(), this.isMerge);
		}

		if (this.newlyAddedNodes != null)
			json.put(Keys.NEWLY_ADDED_NODES.toString(),
					this.mapToArray(newlyAddedNodes));
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
	 * @return A single element set containing the initiator node.
	 */
	public Set<NodeIDType> getInitiatorAsSet() {
		HashSet<NodeIDType> set = new HashSet<NodeIDType>();
		set.add(this.getInitiator());
		return set;
	}

	/**
	 * @return Whether the epoch being created has no previous epoch group or 
	 * it is split or merge reconfiguration operation.
	 */
	public boolean isInitEpoch() {
		return (this.noPrevEpochGroup() && this.epochNumber == 0)
				|| (this.prevGroupName != null && !this.prevGroupName
						.equals(this.getServiceName()));
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
	 * @return Initial state.
	 */
	public String getInitialState() {
		return initialState;
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
	 * @return Set of newly added nodes.
	 */
	public Set<NodeIDType> getNewlyAddedNodes() {
		return this.newlyAddedNodes == null ? new HashSet<NodeIDType>()
				: new HashSet<NodeIDType>(this.newlyAddedNodes.keySet());
	}

	 static void main(String[] args) {
		int[] group = { 3, 45, 6, 19 };
		StartEpoch<Integer> se = new StartEpoch<Integer>(4, "name1", 2,
				Util.arrayToIntSet(group), Util.arrayToIntSet(group));
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
}
