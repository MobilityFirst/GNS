package edu.umass.cs.gigapaxos.paxospackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.paxosutil.Ballot;

/**
 * @author arun
 *
 */
@SuppressWarnings("javadoc")
public final class PreparePacket extends PaxosPacket {

	/**
	 * Ballot in the PREPARE message.
	 */
	public final Ballot ballot;
	/**
	 * First undecided slot at coordinator. Only used as an optimization, not
	 * for safety.
	 */
	public final int firstUndecidedSlot;

	// non-final because AbstractPaxosLogger.rollForward needs to set it
	private boolean recovery;

	public PreparePacket(Ballot b) {
		this(b, -1);
	}

	public PreparePacket(Ballot b, int firstUndecidedSlot) {
		super((PaxosPacket) null);
		this.ballot = b;
		this.packetType = PaxosPacketType.PREPARE;
		this.firstUndecidedSlot = firstUndecidedSlot;
		this.recovery = false;
	}

	public PreparePacket(int coordinatorID, int receiverID, Ballot ballot,
			int slotNumber) {
		super((PaxosPacket) null);
		this.ballot = ballot;
		this.firstUndecidedSlot = slotNumber;
		this.packetType = PaxosPacketType.PREPARE;
		this.recovery = false;
	}

	public PreparePacket(JSONObject json) throws JSONException {
		super(json);
		assert (PaxosPacket.getPaxosPacketType(json) == PaxosPacketType.PREPARE);
		this.packetType = PaxosPacket.getPaxosPacketType(json);
		this.ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT
				.toString()));
		this.firstUndecidedSlot = json
				.getInt(PaxosPacket.Keys.FIRST_UNDECIDED_SLOT.toString());
		this.recovery = json.getBoolean(PaxosPacket.Keys.RECOVERY.toString());
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(), ballot.toString());
		json.put(PaxosPacket.Keys.FIRST_UNDECIDED_SLOT.toString(),
				firstUndecidedSlot);
		json.put(PaxosPacket.Keys.RECOVERY.toString(), recovery);

		return json;
	}

	public boolean isRecovery() {
		return this.recovery;
	}

	public void setRecovery() {
		this.recovery = true;
	}

	@Override
	protected String getSummaryString() {
		return ballot + "(" +this.firstUndecidedSlot+")";
	}
}