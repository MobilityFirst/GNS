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
public abstract class PaxosPacket extends JSONPacket {

	/**
	 * Keys used in json packets.
	 */
	public static enum Keys {
		/**
		 * JSON key for packet type field in JSON representation.
		 */
		PPT,
		/**
		 * JSON key for paxosID field.
		 */
		PID,
		/**
		 * JSON key for paxos version (or epoch number) field.
		 */
		PV,
		/**
		 * slot
		 */
		S,
		/**
		 * median checkpointed slot; used for GC
		 */
		MED_CP_S,
		/**
		 * accepted pvalues from lower ballots
		 */
		ACC_MAP,
		/**
		 * min slot in prepare reply
		 */
		PREPLY_MIN,
		/**
		 * whether recovery mode packet
		 */
		RCVRY,
		/**
		 * max checkpointed slot; used for GC
		 */
		MAX_CP_S,
		/**
		 * checkpoint state
		 */
		STATE,
		/**
		 * max decision slot; used to sync missing decisions
		 */
		MAX_S,
		/**
		 * missing decisions
		 */
		MISSS,
		/**
		 * should checkpoint transfer?; used while sync'ing decisions
		 */
		CP_TX,
		/**
		 * first slot being prepared
		 */
		PREP_MIN,
		/**
		 * large checkpoint option?
		 */
		BIG_CP,
		/**
		 * sync mode; used by pause deactivator
		 */
		SYNCM
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
		/**
		 * 
		 */
		SNDR, 
		/**
		 * 
		 */
		BLLT, 
		/**
		 * 
		 */
		COORD, 
		/**
		 * 
		 */
		ACCPTR, 
		/**
		 * 
		 */
		GROUP, 
		/**
		 * 
		 */
		ENTRY
	}

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
		/**
		 * 
		 */
		RESPONSE("RESPONSE", 0), 
		/**
		 * 
		 */
		REQUEST("REQUEST", 1), 
		/**
		 * 
		 */
		PREPARE("PREPARE", 2), 
		/**
		 * 
		 */
		ACCEPT("ACCEPT", 3), 
		/**
		 * 
		 */
		RESEND_ACCEPT("RESEND_ACCEPT", 4), 
		/**
		 * 
		 */
		PROPOSAL("PROPOSAL", 5), 
		/**
		 * 
		 */
		DECISION("DECISION", 6), 
		/**
		 * 
		 */
		PREPARE_REPLY("PREPARE_REPLY", 7), 
		/**
		 * 
		 */
		ACCEPT_REPLY("ACCEPT_REPLY", 8), 
		/**
		 * 
		 */
		FAILURE_DETECT("FAILURE_DETECT", 9), 
		/**
		 * 
		 */
		PREEMPTED("PREEMPTED", 13), 
		/**
		 * 
		 */
		CHECKPOINT_STATE("CHECKPOINT_STATE", 21), 
		/**
		 * 
		 */
		CHECKPOINT_REQUEST(	"CHECKPOINT_REQUEST", 23), 
		/**
		 * 
		 */
		SYNC_REQUEST("SYNC_REQUEST", 31), 
		/**
		 * 
		 */
		SYNC_DECISIONS("SYNC_DECISIONS", 32), 
		/**
		 * 
		 */
		FIND_REPLICA_GROUP("FIND_REPLICA_GROUP",33), 
		/**
		 * 
		 */
		PAXOS_PACKET("PAXOS_PACKET", 90), 
		/**
		 * 
		 */
		NO_TYPE("NO_TYPE", 9999);

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

		/**
		 * @return String label
		 */
		public String getLabel() {
			return label;
		}

		public String toString() {
			return getLabel();
		}

		/**
		 * @param type
		 * @return Integer type
		 */
		public static PaxosPacketType getPaxosPacketType(int type) {
			return PaxosPacketType.numbers.get(type);
		}

		/**
		 * @param type
		 * @return PaxosPacketType type
		 */
		public static PaxosPacketType getPaxosPacketType(String type) {
			return PaxosPacketType.labels.get(type);
		}
	}

	/**
	 * @param json
	 * @return PaxosPacketType type
	 * @throws JSONException
	 */
	public static PaxosPacketType getPaxosPacketType(JSONObject json)
			throws JSONException {
		return PaxosPacketType.getPaxosPacketType(json
				.getInt(PaxosPacket.Keys.PPT.toString()));
	}

	protected abstract JSONObject toJSONObjectImpl() throws JSONException;

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
			if (json.has(PaxosPacket.Keys.PID.toString()))
				this.paxosID = json.getString(PaxosPacket.Keys.PID.toString());
			if (json.has(PaxosPacket.Keys.PV.toString()))
				this.version = json.getInt(PaxosPacket.Keys.PV.toString());
		}
	}

	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		// tells Packet that this is a PaxosPacket
		JSONPacket.putPacketType(json,
				PaxosPacket.PaxosPacketType.PAXOS_PACKET.getInt());
		// the specific type of PaxosPacket
		json.put(PaxosPacket.Keys.PPT.toString(), this.packetType.getInt());
		json.put(PaxosPacket.Keys.PID.toString(), this.paxosID);
		json.put(PaxosPacket.Keys.PV.toString(), this.version);

		// copy over child fields
		JSONObject child = toJSONObjectImpl();
		for (String name : JSONObject.getNames(child))
			json.put(name, child.get(name));

		return json;
	}

	/**
	 * @return Type
	 */
	public PaxosPacketType getType() {
		return this.packetType;
	}

	/**
	 * @param pid
	 * @param v
	 */
	public void putPaxosID(String pid, int v) {
		this.paxosID = pid;
		this.version = v;
	}

	/**
	 * @return Paxos ID
	 */
	public String getPaxosID() {
		return this.paxosID;
	}

	/**
	 * @return Integer version (or epoch number)
	 */
	public int getVersion() {
		return this.version;
	}

	/**
	 * @param json
	 * @return True if packet generated during recovery mode.
	 * @throws JSONException
	 */
	public static boolean isRecovery(JSONObject json) throws JSONException {
		return json.optBoolean(PaxosPacket.Keys.RCVRY.toString());
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

	protected abstract String getSummaryString();

	/**
	 * @return For pretty printing.
	 */
	public Object getSummary() {
		return new Object() {
			public String toString() {
				return getPaxosID() + ":" + getVersion() + ":" + getType()
						+ ":[" + getSummaryString() + "]";
			}
		};
	}

	/************* Type-specific methods below *******************/
	/**
	 * @param packet
	 * @return PaxosPacket marked as recovery mode.
	 */
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
	 * @param msg 
	 * @return Paxos packet from string.
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
