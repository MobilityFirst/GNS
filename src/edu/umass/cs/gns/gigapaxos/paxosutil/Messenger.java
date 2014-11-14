package edu.umass.cs.gns.gigapaxos.paxosutil;

import java.io.IOException;
import java.util.Set;

import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nio.JSONMessenger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.gigapaxos.multipaxospacket.PaxosPacket;

/**
 * @author V. Arun
 */
/*
 * This class is separate in order to separate communication from the
 * paxos protocol. It has support for retransmissions with exponential
 * backoff. But you can't rely on this backoff for anything other than
 * ephemeral traffic bursts. If you are overloaded, you are overloaded.
 * 
 * It's a utility because there is nothing paxos specific here.
 */
public class Messenger<NodeIDType> extends JSONMessenger<NodeIDType> {

	private final IntegerMap<NodeIDType> nodeMap;

	public Messenger(InterfaceJSONNIOTransport<NodeIDType> niot,
			IntegerMap<NodeIDType> nodeMap) {
		super(niot);
		this.nodeMap = nodeMap;
	}

	public void send(MessagingTask mtask) throws JSONException, IOException {
		// FIXME: need to convert integers to NodeIDType.toString here
		super.send(toGeneric(mtask));
	}

	private JSONObject[] toJSONObjects(PaxosPacket[] msgs) throws JSONException {
		JSONObject[] jsonArray = new JSONObject[msgs.length];
		for (int i = 0; i < msgs.length; i++) {
			jsonArray[i] = fixNodeIntToString(msgs[i].toJSONObject());
		}
		return jsonArray;
	}

	private GenericMessagingTask<NodeIDType, JSONObject> toGeneric(
			MessagingTask mtask) throws JSONException {
		Set<NodeIDType> nodes =
				this.nodeMap.getIntArrayAsNodeSet(mtask.recipients);
		return new GenericMessagingTask<NodeIDType, JSONObject>(
				nodes.toArray(), toJSONObjects(mtask.msgs));
	}

	// convert int to NodeIDType to String
	private JSONObject fixNodeIntToString(JSONObject json) throws JSONException {
		if(json.has(PaxosPacket.NodeIDKeys.BALLOT.toString())) {
			// fix ballot string
			Ballot ballot = new Ballot(json.getString(PaxosPacket.NodeIDKeys.BALLOT.toString()));
			json.put(PaxosPacket.NodeIDKeys.BALLOT.toString(),  Ballot.getBallotString(
				ballot.ballotNumber, intToString(ballot.coordinatorID)));
		}
		else if(json.has(PaxosPacket.NodeIDKeys.GROUP.toString())) {
			// fix group JSONArray
			JSONArray jsonArray = json.getJSONArray(PaxosPacket.NodeIDKeys.GROUP.toString());
			for(int i=0; i<jsonArray.length(); i++) {
				int member = jsonArray.getInt(i);
				jsonArray.put(i, intToString(member));
			}
			json.put(PaxosPacket.NodeIDKeys.GROUP.toString(), jsonArray);
		}
		else 
		for(PaxosPacket.NodeIDKeys key : PaxosPacket.NodeIDKeys.values()) {
			if(json.has(key.toString())) {
				// simple default int->string fix
				int id = json.getInt(key.toString());
				json.put(key.toString(), intToString(id));
			}
		}
		return json;
	}
	private String intToString(int id) {
		return this.nodeMap.get(id).toString();
	}
}
