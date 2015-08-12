package edu.umass.cs.gigapaxos.paxospackets;

import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author arun
 *
 */
public class BatchedAcceptReply extends AcceptReplyPacket {

	/**
	 * 
	 */
	public static final int MAX_BATCH_SIZE = 2048;

	private final TreeSet<Integer> slots = new TreeSet<Integer>();

	/**
	 * @param ar
	 */
	public BatchedAcceptReply(AcceptReplyPacket ar) {
		super(ar.acceptor, ar.ballot, ar.slotNumber, ar.maxCheckpointedSlot);
		this.packetType = PaxosPacket.PaxosPacketType.BATCHED_ACCEPT_REPLY;
		this.putPaxosID(ar.getPaxosID(), ar.getVersion());
		this.slots.add(ar.slotNumber);
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public BatchedAcceptReply(JSONObject json) throws JSONException {
		super(json);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
	
		JSONArray jsonArray = json.getJSONArray(PaxosPacket.Keys.SLOTS.toString());
		for (int i = 0; i < jsonArray.length(); i++)
			this.slots.add(jsonArray.getInt(i));
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(PaxosPacket.Keys.SLOTS.toString(), this.slots);
		return json;
	}
	
	/**
	 * @param ar
	 * @return True if no entry already present for the slot.
	 */
	public boolean addAcceptReply(AcceptReplyPacket ar) {
		if (!ar.ballot.equals(this.ballot) || !this.paxosID.equals(ar.paxosID))
			throw new RuntimeException("Unable to combine " + ar.getSummary()
					+ " with " + this.getSummary());
		return this.slots.add(ar.slotNumber);
	}

	/**
	 * @return Accepted slots.
	 */
	public Integer[] getAcceptedSlots() {
		return this.slots.toArray(new Integer[0]);
	}
}
