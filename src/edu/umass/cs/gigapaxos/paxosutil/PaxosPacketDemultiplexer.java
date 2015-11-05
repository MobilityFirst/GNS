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
package edu.umass.cs.gigapaxos.paxosutil;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.paxospackets.AcceptPacket;
import edu.umass.cs.gigapaxos.paxospackets.AcceptReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.BatchedAcceptReply;
import edu.umass.cs.gigapaxos.paxospackets.BatchedCommit;
import edu.umass.cs.gigapaxos.paxospackets.FailureDetectionPacket;
import edu.umass.cs.gigapaxos.paxospackets.FindReplicaGroupPacket;
import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.gigapaxos.paxospackets.PreparePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.ProposalPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.gigapaxos.paxospackets.StatePacket;
import edu.umass.cs.gigapaxos.paxospackets.SyncDecisionsPacket;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.utils.Config;

/**
 * @author V. Arun
 *         <p>
 *         Used to get NIO to send paxos packets to PaxosManager. This class has
 *         been merged into PaxosManager now and will be soon deprecated.
 */
public abstract class PaxosPacketDemultiplexer extends
		AbstractPacketDemultiplexer<JSONObject> {

	/**
	 * @param json
	 * @param unstringer
	 * @return PaxosPacket parsed from JSON.
	 * @throws JSONException
	 */
	public static PaxosPacket toPaxosPacket(JSONObject json,
			Stringifiable<?> unstringer) throws JSONException {
		PaxosPacket.PaxosPacketType type = PaxosPacket.getPaxosPacketType(json);
		if (type == null)
			fatal(json);

		PaxosPacket paxosPacket = null;
		switch (type) {
		case REQUEST:
			paxosPacket = (new RequestPacket(json));
			break;
		case PROPOSAL:
			paxosPacket = (new ProposalPacket(json));
			break;
		case DECISION:
			paxosPacket = (new PValuePacket(json));
			break;
		case BATCHED_COMMIT:
			paxosPacket = (new BatchedCommit(json));
			break;
		case PREPARE:
			paxosPacket = (new PreparePacket(json));
			break;
		case PREPARE_REPLY:
			paxosPacket = (new PrepareReplyPacket(json));
			break;
		case ACCEPT:
			paxosPacket = (new AcceptPacket(json));
			break;
		case ACCEPT_REPLY:
			paxosPacket = (new AcceptReplyPacket(json));
			break;
		case BATCHED_ACCEPT_REPLY:
			paxosPacket = (new BatchedAcceptReply(json));
			break;
		case SYNC_DECISIONS:
			paxosPacket = (new SyncDecisionsPacket(json));
			break;
		case CHECKPOINT_STATE:
			paxosPacket = (new StatePacket(json));
			break;
		case FAILURE_DETECT:
			paxosPacket = new FailureDetectionPacket<>(json, unstringer);
			break;
		case FIND_REPLICA_GROUP:
			paxosPacket = new FindReplicaGroupPacket(json);
			break;
		default:
			fatal(json);
		}
		assert (paxosPacket != null) : json;
		return paxosPacket;
	}
	
	private static final double THROTTLE_SLEEP = Config.getGlobalDouble(PC.THROTTLE_SLEEP);
	/**
	 * 
	 */
	public static void throttleExcessiveLoad() {
		try {
			Thread.sleep(THROTTLE_SLEEP >= 1 ? (long) THROTTLE_SLEEP : (Math
					.random() < THROTTLE_SLEEP ? 1 : 0));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}



	private static void fatal(Object json) {
		PaxosManager.getLogger().severe(
				PaxosPacketDemultiplexer.class.getSimpleName() + " received "
						+ json);
		throw new RuntimeException(
				"PaxosPacketDemultiplexer recieved unrecognized paxos packet type");
	}
}
