package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxospackets.PaxosPacket;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.Stringifiable;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * @author V. Arun Needed to get NIO to send paxos packets to PaxosManager
 * */
@SuppressWarnings("javadoc")
public class PaxosPacketDemultiplexer<NodeIDType> extends
		AbstractJSONPacketDemultiplexer {

	private final PaxosManager<NodeIDType> paxosManager;
	private final IntegerMap<NodeIDType> nodeMap;
	private final Stringifiable<NodeIDType> unstringer;

	public PaxosPacketDemultiplexer(PaxosManager<NodeIDType> pm,
			IntegerMap<NodeIDType> nodeMap, Stringifiable<NodeIDType> unstringer) {
		paxosManager = pm;
		this.nodeMap = nodeMap;
		this.unstringer = unstringer;
		this.register(PaxosPacket.PaxosPacketType.PAXOS_PACKET);
	}

	public boolean handleMessage(JSONObject jsonMsg) {
		boolean isPacketTypeFound = true;

		try {
			PaxosPacket.PaxosPacketType type = PaxosPacket.PaxosPacketType
					.getPaxosPacketType(JSONPacket.getPacketType(jsonMsg));
			switch (type) {
			case PAXOS_PACKET:
				paxosManager.handleIncomingPacket((jsonMsg));
				break;
			default:
				isPacketTypeFound = false;
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
		return isPacketTypeFound;
	}

	/*
	 * FIXME: This method will be removed as it is no longer used. The
	 * corresponding functionality has been moved inside handleIncoming in
	 * PaxosManager.
	 */
	// convert string -> NodeIDType -> int (can *NOT* convert string directly to
	// int)
	@Deprecated
	protected JSONObject fixNodeStringToInt(JSONObject json)
			throws JSONException {
		// FailureDetectionPacket already has generic NodeIDType
		if (PaxosPacket.getPaxosPacketType(json) == PaxosPacket.PaxosPacketType.FAILURE_DETECT)
			return json;

		if (json.has(PaxosPacket.NodeIDKeys.BALLOT.toString())) {
			// fix ballot string
			String ballotString = json.getString(PaxosPacket.NodeIDKeys.BALLOT
					.toString());
			Integer coordInt = this.nodeMap.put(this.unstringer.valueOf(Ballot
					.getBallotCoordString(ballotString)));
			assert (coordInt != null);
			Ballot ballot = new Ballot(Ballot.getBallotNumString(ballotString),
					coordInt);
			json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(),
					ballot.toString());
		} else if (json.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			// fix group string (JSONArray)
			JSONArray jsonArray = json
					.getJSONArray(PaxosPacket.NodeIDKeys.GROUP.toString());
			for (int i = 0; i < jsonArray.length(); i++) {
				String memberString = jsonArray.getString(i);
				int memberInt = this.nodeMap.put(this.unstringer
						.valueOf(memberString));
				jsonArray.put(i, memberInt);
			}
		} else
			for (PaxosPacket.NodeIDKeys key : PaxosPacket.NodeIDKeys.values()) {
				if (json.has(key.toString())) {
					// fix default node string
					String nodeString = json.getString(key.toString());
					int nodeInt = this.nodeMap.put(this.unstringer
							.valueOf(nodeString));
					json.put(key.toString(), nodeInt);
				}
			}
		return json;
	}

}
