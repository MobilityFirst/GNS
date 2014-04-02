package edu.umass.cs.gns.replicaCoordination.multipaxos;

import edu.umass.cs.gns.nio.PacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import org.json.JSONException;
import org.json.JSONObject;

/**
@author V. Arun
 */
public class PaxosPacketDemultiplexer extends PacketDemultiplexer {

	private final PaxosManager paxosManager;

	PaxosPacketDemultiplexer(PaxosManager pm) {
		paxosManager = pm;
	}

//	@Override
//	public void handleJSONObjects(ArrayList<JSONObject> jsonObjects) {
//		for (Object j : jsonObjects) {
//			handleJSONObject((JSONObject) j);
//		}
//
//	}
	
	public boolean handleJSONObject(JSONObject jsonMsg) {
    boolean isPacketTypeFound = true;
		try {
			Packet.PacketType type = Packet.getPacketType(jsonMsg);
			switch (type) {
			case PAXOS_PACKET:
				paxosManager.handleIncomingPacket(jsonMsg);
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

	/**
	 * @param args
	 */
	 public static void main(String[] args) {
		// TODO Auto-generated method stub

	 }

}
