package edu.umass.cs.gns.replicaCoordination.multipaxos;

import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nameserver.NameServer;
import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.packet.Packet;

/**
@author V. Arun
 */
public class PaxosPacketDemultiplexer extends PacketDemultiplexer {

	private final PaxosManager paxosManager;

	PaxosPacketDemultiplexer(PaxosManager pm) {
		paxosManager = pm;
	}

	@Override
	public void handleJSONObjects(ArrayList<JSONObject> jsonObjects) {
		for (Object j : jsonObjects) {
			handleJSONObject((JSONObject) j);
		}

	}
	
	public void handleJSONObject(JSONObject jsonMsg) {
		try {
			Packet.PacketType type = Packet.getPacketType(jsonMsg);
			switch (type) {
			case PAXOS_PACKET:
				paxosManager.handleIncomingPacket(jsonMsg);
				break;
			}
		} catch (JSONException e) {
			e.printStackTrace();
		} 
	}

	/**
	 * @param args
	 */
	 public static void main(String[] args) {
		// TODO Auto-generated method stub

	 }

}
