package edu.umass.cs.gns.nsdesign.packet;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;
import edu.umass.cs.gns.nsdesign.packet.Packet;
/**************** FIXME Package deprecated by nsdesign/packet. this will soon be deleted. **/
/**  
@author V. Arun. 
 */
public abstract class PaxosPacket extends Packet {

	public static final String PAXOS_TYPE = "PAXOS_TYPE"; // Name of packet type field in JSON representation
	public static final String PAXOS_ID = "PAXOS_ID";
	public static final String PAXOS_VERSION = "PAXOS_VERSION";
	
	protected static final String RECOVERY = "RECOVERY"; // macro used by prepare, accept, pvalue

	/* Every PaxosPacket has a minimum of the following three fields.
	 * The fields paxosID and version are preserved across 
	 * inheritances, e.g., ProposalPacket gets them from the 
	 * corresponding RequestPacket it is extending. 
	 */
	protected PaxosPacketType packetType;
	protected String paxosID=null;
	protected short version=-1;

	/* The enum type below could be merged with Packet. There
	 * is little reason to have a special TYPE called PAXOS_PACKET in Packet
	 * and then another field for sub-types indicating the PAXOS_TYPE.
	 */
	public enum PaxosPacketType {
		RESPONSE ("RESPONSE", 0),
		REQUEST ("REQUEST", 1),
		PREPARE ("PREPARE", 2),
		ACCEPT ("ACCEPT", 3),
		RESEND_ACCEPT ("RESEND_ACCEPT", 4), 
		PROPOSAL ("PROPOSAL", 5),
		DECISION ("DECISION", 6),
		PREPARE_REPLY ("PREPARE_REPLY", 7),
		ACCEPT_REPLY ("ACCEPT_REPLY", 8),
		FAILURE_DETECT ("FAILURE_DETECT", 9),
		PREEMPTED ("PREEMPTED", 13),
		CHECKPOINT_STATE ("CHECKPOINT_STATE", 21),
		CHECKPOINT_REQUEST ("CHECKPOINT_REQUEST", 23),
		SYNC_REQUEST ("SYNC_REQUEST", 31),
		SYNC_REPLY ("SYNC_REPLY", 32), 
		FIND_REPLICA_GROUP ("FIND_REPLICA_GROUP", 33);

		private static HashMap<String,PaxosPacketType> labels = new HashMap<String,PaxosPacketType>();
		private static HashMap<Integer,PaxosPacketType> numbers = new HashMap<Integer,PaxosPacketType>();

		private final String label;
		private final int number;

		PaxosPacketType(String s, int t) {
			this.label = s;
			this.number = t;
		}

		/************** BEGIN static code block to ensure correct initialization **************/
		static {
			for(PaxosPacketType type : PaxosPacketType.values()) {
				if(!PaxosPacketType.labels.containsKey(type.label) && !PaxosPacketType.numbers.containsKey(type.number)) {
					PaxosPacketType.labels.put(type.label, type);
					PaxosPacketType.numbers.put(type.number, type);
				} else {
					assert(false) : "Duplicate or inconsistent enum type";
					throw new RuntimeException("Duplicate or inconsistent enum type");
				}
			}
		}
		/**************** END static code block to ensure correct initialization **************/

		public int getNumber() {
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

	public static PaxosPacketType getPaxosPacketType(JSONObject json) throws JSONException {
		return PaxosPacketType.getPaxosPacketType(json.getInt(PaxosPacket.PAXOS_TYPE));
	}

	public abstract JSONObject toJSONObjectImpl() throws JSONException;

	/* PaxosPacket has no no-arg constructor for a good reason. All
	 * classes extending PaxosPacket must in their constructors 
	 * invoke super(JSONObject) or super(PaxosPacket) as a PaxosPacket
	 * is typically created only in one of those two ways. If a 
	 * PaxosPacket is being created from nothing, the child should
	 * explicitly acknowledge that fact by invoking 
	 * super((PaxosPacket)null), thereby acknowledging that paxosID
	 * is not set and is not meant to be set. Otherwise it is all 
	 * too easy to forget to stamp a paxosID into a PaxosPacket. 
	 * This is problematic when we create a new PaxosPacket 
	 * internally and consume it internally within a paxos instance. 
	 * A PaxosPacket that is coming in and, by consequence, any 
	 * PaxosPacket that has been sent out of a paxos instance will 
	 * have a good paxosID, so we don't need to worry about those.
	 */
	protected PaxosPacket(PaxosPacket pkt) {
		if(pkt!=null) {
			this.paxosID = pkt.paxosID;
			this.version = pkt.version;
		}
	}
	protected PaxosPacket(JSONObject json) throws JSONException {
		// Need to call Packet.putPacketType only when going to (not from) JSON
		if(json!=null) {
			if(json.has(PaxosPacket.PAXOS_ID)) this.paxosID = json.getString(PaxosPacket.PAXOS_ID);
			if(json.has(PaxosPacket.PAXOS_VERSION)) this.version = (short)json.getInt(PaxosPacket.PAXOS_VERSION);
		}
	}

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, PacketType.PAXOS_PACKET); // tells Packet that this is a PaxosPacket
		json.put(PaxosPacket.PAXOS_TYPE, this.packetType.getNumber()); // the specific type of PaxosPacket
		json.put(PaxosPacket.PAXOS_ID, this.paxosID);

		// copy over child fields, can also call child first and copy parent fields 
		JSONObject child = toJSONObjectImpl();
		for(String name : JSONObject.getNames(child)) {
			json.put(name, child.get(name));
		}

		return json;
	}
	public PaxosPacketType getType() {return this.packetType;}

	public void putPaxosID(String pid, short v) {
		this.paxosID = pid;
		this.version = v;
	}
	public String getPaxosID() {
		return this.paxosID;
	}
	public short getVersion() {
		return this.version;
	}
	
	public static String setRecovery(PaxosPacket packet) throws JSONException {
		JSONObject json = packet.toJSONObject();
		json.put(RECOVERY, true);
		return json.toString();
	}
	public static boolean isRecovery(JSONObject json) throws JSONException {
		return json.has(RECOVERY) ? json.getBoolean(RECOVERY) : false;
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
}
