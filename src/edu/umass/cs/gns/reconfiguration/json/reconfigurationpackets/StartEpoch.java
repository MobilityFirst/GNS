package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.Stringifiable;
import edu.umass.cs.gns.nio.StringifiableDefault;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 */
public class StartEpoch<NodeIDType> extends
		BasicReconfigurationPacket<NodeIDType> {

	public static enum Keys {
		PREV_EPOCH_GROUP, CUR_EPOCH_GROUP, CREATOR, INITIAL_STATE, PREV_GROUP_NAME, NEWLY_ADDED_NODES, NODE_ID, SOCKET_ADDRESS, PREV_EPOCH, IS_MERGE
	};

	public final Set<NodeIDType> prevEpochGroup;
	public final Set<NodeIDType> curEpochGroup;
	public final String initialState;

	public final InetSocketAddress creator; // for creation (or first) epoch

	public final String prevGroupName; // for merge or split group operations
	public final int prevEpoch;

	public final boolean isMerge;

	// used only in case of new RC node addition
	public final Map<NodeIDType, InetSocketAddress> newlyAddedNodes;

	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes, Set<NodeIDType> prevNodes) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes, null,
				false, -1, null, null, null);
	}

	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, String prevGroupName, boolean isMerge,
			int prevEpoch) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes,
				prevGroupName, isMerge, prevEpoch, null, null, null);
	}

	public StartEpoch(NodeIDType initiator, String serviceName,
			int epochNumber, Set<NodeIDType> curNodes,
			Set<NodeIDType> prevNodes, InetSocketAddress creator,
			String initialState,
			Map<NodeIDType, InetSocketAddress> newlyAddedNodes) {
		this(initiator, serviceName, epochNumber, curNodes, prevNodes, null,
				false, -1, creator, initialState, newlyAddedNodes);
	}

	public StartEpoch(StartEpoch<NodeIDType> startEpoch, String initialState) {
		this(startEpoch.getInitiator(), startEpoch.getServiceName(), startEpoch
				.getEpochNumber(), startEpoch.curEpochGroup,
				startEpoch.prevEpochGroup, startEpoch.prevGroupName,
				startEpoch.isMerge, startEpoch.prevEpoch, startEpoch.creator,
				initialState, startEpoch.newlyAddedNodes);
	}

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

	public Set<NodeIDType> getCurEpochGroup() {
		return this.curEpochGroup;
	}

	public Set<NodeIDType> getPrevEpochGroup() {
		return this.prevEpochGroup;
	}

	public Set<NodeIDType> getInitiatorAsSet() {
		HashSet<NodeIDType> set = new HashSet<NodeIDType>();
		set.add(this.getInitiator());
		return set;
	}

	public boolean isInitEpoch() {
		return (this.noPrevEpochGroup() && this.epochNumber == 0)
				|| (this.prevGroupName != null && !this.prevGroupName
						.equals(this.getServiceName()));
	}

	public boolean noCurEpochGroup() {
		return this.curEpochGroup == null || this.curEpochGroup.isEmpty();
	}

	public boolean noPrevEpochGroup() {
		return this.prevEpochGroup == null || this.prevEpochGroup.isEmpty();
	}

	public boolean hasPrevEpochGroup() {
		return !this.noPrevEpochGroup();
	}

	public String getInitialState() {
		return initialState;
	}

	public String getPrevGroupName() {
		return this.prevGroupName != null ? this.prevGroupName : this
				.getServiceName();
	}

	public int getPrevEpochNumber() {
		return this.prevGroupName != null ? this.prevEpoch : (this
				.getEpochNumber() - 1);
	}

	public boolean isSplitOrMerge() {
		return this.getPrevGroupName() != null
				&& !this.getPrevGroupName().equals(this.getServiceName());
	}

	public boolean hasNewlyAddedNodes() {
		return this.newlyAddedNodes != null && !this.newlyAddedNodes.isEmpty();
	}

	public boolean isMerge() {
		return this.isMerge;
	}

	public boolean isCreate() {
		return !this.isMerge;
	}

	public Set<NodeIDType> getNewlyAddedNodes() {
		return this.newlyAddedNodes == null ? new HashSet<NodeIDType>()
				: new HashSet<NodeIDType>(this.newlyAddedNodes.keySet());
	}

	public static void main(String[] args) {
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
