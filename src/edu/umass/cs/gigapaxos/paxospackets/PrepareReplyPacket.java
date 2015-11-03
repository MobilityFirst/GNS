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
package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public class PrepareReplyPacket extends PaxosPacket {

	/**
	 * Ballot of the PREPARE being replied to.
	 */
	public final Ballot ballot;
	/**
	 * Node ID of replier.
	 */
	public final int acceptor;
	/**
	 * Accepted pvalues from lower ballots.
	 */
	public final TreeMap<Integer, PValuePacket> accepted;

	// first pvalue slot in accepted pvalues
	private final int firstSlot;

	/*
	 * Maximum pvalue slot in accepted pvalues. We store this explicitly even
	 * though it can be computed from the accepted pvalues map in order to
	 * support fragmentation, wherein the accepted pvalues map may not contain
	 * all of the pvalues needed for safety.
	 */
	private final int maxSlot;
	
	/* Different from firstSlot in that firstSlot is gcSlot+1, but minSlot
	 * is the minimum slot in accepted when this prepare reply was first
	 * created.
	 */
	private final int minSlot;

	private long createTime = System.currentTimeMillis();

	public PrepareReplyPacket(int receiverID, Ballot ballot,
			Map<Integer, PValuePacket> accepted, int gcSlot, int minSlot, int maxSlot) {
		super(accepted == null || accepted.isEmpty() ? (PaxosPacket) null
				: accepted.values().iterator().next());
		this.acceptor = receiverID;
		this.ballot = ballot;
		this.accepted = accepted == null ? new TreeMap<Integer, PValuePacket>()
				: new TreeMap<Integer, PValuePacket>(accepted);
		this.firstSlot = gcSlot + 1;
		this.maxSlot = maxSlot;
		this.minSlot = minSlot;
		this.packetType = PaxosPacketType.PREPARE_REPLY;
	}

	public PrepareReplyPacket(int receiverID, Ballot ballot,
			Map<Integer, PValuePacket> accepted, int gcSlot) {
		this(receiverID, ballot, accepted, gcSlot, getMinSlot(gcSlot+1, accepted), getMaxSlot(gcSlot + 1,
				accepted));
	}

	public PrepareReplyPacket(JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.PREPARE_REPLY);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.acceptor = json.getInt(PaxosPacket.NodeIDKeys.ACCPTR.toString());
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.B
				.toString()));
		this.accepted = parseJsonForAccepted(json);
		this.firstSlot = json.getInt(PaxosPacket.Keys.PREPLY_MIN.toString());
		this.maxSlot = json.getInt(PaxosPacket.Keys.MAX_S.toString());
		this.minSlot = json.getInt(PaxosPacket.Keys.MIN_S.toString());
		this.createTime = json.getLong(RequestPacket.Keys.CT.toString());
	}

	// only for unit testing in PrepareReplyAssembler
	public PrepareReplyPacket(int acceptor, Ballot ballot,
			HashMap<Integer, PValuePacket> acceptedMap, int gcSlot, int max) {
		this(acceptor, ballot, acceptedMap, gcSlot, gcSlot+1, max);
	}

	private TreeMap<Integer, PValuePacket> parseJsonForAccepted(JSONObject json)
			throws JSONException {
		TreeMap<Integer, PValuePacket> accepted = new TreeMap<Integer, PValuePacket>();
		if (json.has(PaxosPacket.Keys.ACC_MAP.toString())) {
			JSONArray jsonArray = json.getJSONArray(PaxosPacket.Keys.ACC_MAP
					.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				JSONObject element = jsonArray.getJSONObject(i);
				PValuePacket pval = new PValuePacket(element);
				accepted.put(pval.slot, pval);
			}
		}
		return accepted;
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.ACCPTR.toString(), acceptor);
		json.put(PaxosPacket.NodeIDKeys.B.toString(), ballot.toString());
		assert (this.packetType == PaxosPacketType.PREPARE_REPLY);
		addAcceptedToJSON(json);
		json.put(PaxosPacket.Keys.PREPLY_MIN.toString(), this.firstSlot);
		json.put(PaxosPacket.Keys.MAX_S.toString(), this.maxSlot);
		json.put(PaxosPacket.Keys.MIN_S.toString(), this.minSlot);
		json.put(RequestPacket.Keys.CT.toString(), this.createTime);
		return json;
	}

	private int getMaxSlot() {
		return getMaxSlot(this.firstSlot, this.accepted);
	}

	public int getMinSlot() {
		return getMinSlot(this.accepted);
	}

	private int getMinSlot(Map<Integer, PValuePacket> acceptedMap) {
		Integer minSlot = this.firstSlot;
		if (acceptedMap != null && !acceptedMap.isEmpty()) {
			for (Integer curSlot : acceptedMap.keySet()) {
				if (curSlot - minSlot < 0)
					minSlot = curSlot;
			}
		} 
		return minSlot;
	}

	private static int getMinSlot(int firstSlot,
			Map<Integer, PValuePacket> acceptedMap) {
		Integer minSlot = null;
		if(acceptedMap!=null && !acceptedMap.isEmpty()) {
			for(Integer curSlot : acceptedMap.keySet()) {
				if(minSlot == null) minSlot = curSlot;
				else if(curSlot - minSlot < 0) minSlot = curSlot; 
			}
		}
		if(minSlot==null) minSlot = firstSlot;
		return minSlot;
	}

	// FIXME: wraparound
	private static int getMaxSlot(int firstSlot,
			Map<Integer, PValuePacket> acceptedMap) {
		Integer maxSlot = firstSlot - 1;
		if (acceptedMap != null && !acceptedMap.isEmpty()) {
			for (Integer curSlot : acceptedMap.keySet())
				if (curSlot - maxSlot > 0)
					maxSlot = curSlot;
		}
		return maxSlot;
	}

	private void addAcceptedToJSON(JSONObject json) throws JSONException {
		if (accepted != null) {
			JSONArray jsonArray = new JSONArray();
			for (PValuePacket pValue : accepted.values()) {
				jsonArray.put(pValue.toJSONObject());
			}
			json.put(PaxosPacket.Keys.ACC_MAP.toString(), jsonArray);
		}
	}

	@Override
	protected String getSummaryString() {
		return acceptor
				+ ":"
				+ ballot
				+ (!accepted.isEmpty() ? ", |accepted|=" + accepted.size()
						+ "[" + this.getMinSlot() + "-" + this.getMaxSlot()
						+ "]" : "");
	}

	public boolean isComplete() {
		for (int i = this.minSlot; i <= this.maxSlot; i++)
			if (!this.accepted.containsKey(i))
				return false;
		return true;
	}

	public boolean combine(PrepareReplyPacket incoming) {
		if (incoming.ballot.compareTo(this.ballot) != 0)
			return false;
		for (int slot : incoming.accepted.keySet())
			this.accepted.put(slot, incoming.accepted.get(slot));
		return this.isComplete();
	}

	public long getCreateTime() {
		return this.createTime;
	}

	public int getLengthEstimate() {
		int size = 0;
		for (PValuePacket pvalue : this.accepted.values())
			size += pvalue.lengthEstimate();
		return size;
	}

	// modifies self
	public PrepareReplyPacket fragment(int length) {
		PrepareReplyPacket frag = new PrepareReplyPacket(this.acceptor,
				this.ballot, new HashMap<Integer, PValuePacket>(),
				this.firstSlot - 1, this.minSlot, this.maxSlot);
		frag.putPaxosID(this.getPaxosID(), this.getVersion());
		int curLength = 0;
		//System.out.println("creating fragment of length "+ length);
		for (Iterator<Integer> slotIter = this.accepted.keySet().iterator(); slotIter
				.hasNext();) {
			PValuePacket pvalue = this.accepted.get(slotIter.next());
			// will create at least one fragment
			if ((curLength += pvalue.lengthEstimate()) > length && !frag.accepted.isEmpty()) {
				//System.out.println("breaking because curLength would become " + curLength);
				break;
			}

			// put into frag and remove from this
			frag.accepted.put(pvalue.slot, pvalue);
			slotIter.remove();
		}
		//System.out.println("returning " + frag.getSummary());
		return frag;
	}
}