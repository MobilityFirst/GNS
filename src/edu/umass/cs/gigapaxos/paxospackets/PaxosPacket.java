package edu.umass.cs.gigapaxos.paxospackets;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.JSONPacket;

/**
 * 
 * @author arun
 *
 *         The parent class for all paxos packet types. Every paxos packet has a
 *         minimum of three fields: packet type, paxos group ID, version.
 */
@SuppressWarnings("javadoc")
public abstract class PaxosPacket extends JSONPacket {

	protected static enum Keys {
		PAXOS_PACKET_TYPE, PAXOS_ID, PAXOS_VERSION, SLOT, MEDIAN_CHECKPOINTED_SLOT, ACCEPTED_MAP, PREPARE_REPLY_FIRST_SLOT, RECOVERY, MAX_CHECKPOINTED_SLOT, STATE, MAX_SLOT, MISSING, IS_MISSING_TOO_MUCH, FIRST_UNDECIDED_SLOT, IS_LARGE_CHECKPOINT
	}

	/**
	 * These are keys involving NodeIDType that need to be "fixed" by Messenger
	 * just before sending and by PaxosPacketDemultiplexer just after being
	 * received. The former fix involves a int->NodeIDType->String conversion
	 * and the latter involves a String->NodeIDType->int conversion. The former
	 * requires IntegerMap<NodeIDType> and the latter requires
	 * Stringifiable<NodeIDTpe>.
	 */
	public static enum NodeIDKeys {
		SENDER_NODE, BALLOT, COORDINATOR, ACCEPTOR, GROUP, DECISION_ISSUER, ENTRY_REPLICA
	}

	/**
	 * JSON key for packet type field in JSON representation.
	 */
	public static final String PAXOS_PACKET_TYPE = "PAXOS_PACKET_TYPE";
	/**
	 * JSON key for paxosID field.
	 */
	public static final String PAXOS_ID = "PAXOS_ID";
	/**
	 * JSON key for paxos version (or epoch number) field.
	 */
	public static final String PAXOS_VERSION = "PAXOS_VERSION";

	public static final String MARK_ACTIVE = "DONT_MARK_ACTIVE";
	/*
	 * Every PaxosPacket has a minimum of the following three fields. The fields
	 * paxosID and version are preserved across inheritances, e.g.,
	 * ProposalPacket gets them from the corresponding RequestPacket it is
	 * extending.
	 */
	protected PaxosPacketType packetType;
	protected String paxosID = null;
	protected int version = -1;

	/**
	 * The paxos packet type class.
	 */
	public enum PaxosPacketType implements IntegerPacketType {
		RESPONSE("RESPONSE", 0), REQUEST("REQUEST", 1), PREPARE("PREPARE", 2), ACCEPT(
				"ACCEPT", 3), RESEND_ACCEPT("RESEND_ACCEPT", 4), PROPOSAL(
				"PROPOSAL", 5), DECISION("DECISION", 6), PREPARE_REPLY(
				"PREPARE_REPLY", 7), ACCEPT_REPLY("ACCEPT_REPLY", 8), FAILURE_DETECT(
				"FAILURE_DETECT", 9), PREEMPTED("PREEMPTED", 13), CHECKPOINT_STATE(
				"CHECKPOINT_STATE", 21), CHECKPOINT_REQUEST(
				"CHECKPOINT_REQUEST", 23), SYNC_REQUEST("SYNC_REQUEST", 31), SYNC_DECISIONS(
				"SYNC_DECISIONS", 32), FIND_REPLICA_GROUP("FIND_REPLICA_GROUP",
				33), PAXOS_PACKET("PAXOS_PACKET", 90), NO_TYPE("NO_TYPE", 9999);

		private static HashMap<String, PaxosPacketType> labels = new HashMap<String, PaxosPacketType>();
		private static HashMap<Integer, PaxosPacketType> numbers = new HashMap<Integer, PaxosPacketType>();

		private final String label;
		private final int number;

		PaxosPacketType(String s, int t) {
			this.label = s;
			this.number = t;
		}

		/************** BEGIN static code block to ensure correct initialization **************/
		static {
			for (PaxosPacketType type : PaxosPacketType.values()) {
				if (!PaxosPacketType.labels.containsKey(type.label)
						&& !PaxosPacketType.numbers.containsKey(type.number)) {
					PaxosPacketType.labels.put(type.label, type);
					PaxosPacketType.numbers.put(type.number, type);
				} else {
					assert (false) : "Duplicate or inconsistent enum type";
					throw new RuntimeException(
							"Duplicate or inconsistent enum type");
				}
			}
		}

		/**************** END static code block to ensure correct initialization **************/

		public int getInt() {
			return number;
		}

		public String getLabel() {
			return label;
		}

		public String toString() {
			return getLabel();
		}

		public static PaxosPacketType getPaxosPacketType(int type) {
			return PaxosPacketType.numbers.get(type);
		}

		public static PaxosPacketType getPaxosPacketType(String type) {
			return PaxosPacketType.labels.get(type);
		}
	}

	public static PaxosPacketType getPaxosPacketType(JSONObject json)
			throws JSONException {
		return PaxosPacketType.getPaxosPacketType(json
				.getInt(PaxosPacket.PAXOS_PACKET_TYPE));
	}

	public abstract JSONObject toJSONObjectImpl() throws JSONException;

	/*
	 * PaxosPacket has no no-arg constructor for a good reason. All classes
	 * extending PaxosPacket must in their constructors invoke super(JSONObject)
	 * or super(PaxosPacket) as a PaxosPacket is typically created only in one
	 * of those two ways. If a PaxosPacket is being created from nothing, the
	 * child should explicitly acknowledge that fact by invoking
	 * super((PaxosPacket)null), thereby acknowledging that paxosID is not set
	 * and is not meant to be set. Otherwise it is all too easy to forget to
	 * stamp a paxosID into a PaxosPacket. This is problematic when we create a
	 * new PaxosPacket internally and consume it internally within a paxos
	 * instance. A PaxosPacket that is coming in and, by consequence, any
	 * PaxosPacket that has been sent out of a paxos instance will have a good
	 * paxosID, so we don't need to worry about those.
	 */
	protected PaxosPacket(PaxosPacket pkt) {
		super(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
		if (pkt != null) {
			this.paxosID = pkt.paxosID;
			this.version = pkt.version;
		}
	}

	protected PaxosPacket(JSONObject json) throws JSONException {
		super(json);
		if (json != null) {
			if (json.has(PaxosPacket.PAXOS_ID))
				this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
			if (json.has(PaxosPacket.PAXOS_VERSION))
				this.version = json.getInt(PaxosPacket.PAXOS_VERSION);
		}
	}

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		// tells Packet that this is a PaxosPacket
		JSONPacket.putPacketType(json,
				PaxosPacket.PaxosPacketType.PAXOS_PACKET.getInt());
		// the specific type of PaxosPacket
		json.put(PaxosPacket.PAXOS_PACKET_TYPE, this.packetType.getInt());
		json.put(PaxosPacket.PAXOS_ID, this.paxosID);
		json.put(PaxosPacket.PAXOS_VERSION, this.version);

		// copy over child fields
		JSONObject child = toJSONObjectImpl();
		for (String name : JSONObject.getNames(child))
			json.put(name, child.get(name));

		return json;
	}

	public PaxosPacketType getType() {
		return this.packetType;
	}

	public void putPaxosID(String pid, int v) {
		this.paxosID = pid;
		this.version = v;
	}

	public String getPaxosID() {
		return this.paxosID;
	}

	public int getVersion() {
		return this.version;
	}

	public static boolean isRecovery(JSONObject json) throws JSONException {
		return json.has(PaxosPacket.Keys.RECOVERY.toString()) ? json
				.getBoolean(PaxosPacket.Keys.RECOVERY.toString()) : false;
	}

	@Override
	public String toString() {
		try {
			return this.toJSONObject().toString();
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
	}

	/************* Type-specific methods below *******************/
	public static PaxosPacket markRecovered(PaxosPacket packet) {
		PaxosPacket.PaxosPacketType type = packet.getType();
		switch (type) {
		case PREPARE:
			((PreparePacket) packet).setRecovery();
			break;
		case ACCEPT:
			((AcceptPacket) packet).setRecovery();
			break;
		case DECISION:
			((PValuePacket) packet).setRecovery();
			break;
		default:
			break;
		}
		return packet;
	}

	/**
	 * Returns a PaxosPacket if parseable from a string.
	 * 
	 * @throws JSONException
	 */
	public static PaxosPacket getPaxosPacket(String msg) throws JSONException {
		PaxosPacket paxosPacket = null;
		JSONObject jsonMsg = new JSONObject(msg);
		PaxosPacketType type = PaxosPacket.getPaxosPacketType(jsonMsg);
		switch (type) {
		case PREPARE:
			paxosPacket = new PreparePacket(jsonMsg);
			break;
		case ACCEPT:
			paxosPacket = new AcceptPacket(jsonMsg);
			break;
		case DECISION:
			paxosPacket = new PValuePacket(jsonMsg);
			break;
		default:
			assert (false);
		}
		return paxosPacket;
	}
	/************* End of type-specific methods *******************/
}
