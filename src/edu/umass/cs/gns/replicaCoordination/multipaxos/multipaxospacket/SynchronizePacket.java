package edu.umass.cs.gns.replicaCoordination.multipaxos.multipaxospacket;

import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.PaxosPacket;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Created with IntelliJ IDEA.
 * User: abhigyan
 * Date: 7/5/13
 * Time: 7:28 PM
 * To change this template use File | Settings | File Templates.
 */
public class SynchronizePacket extends PaxosPacket{

    public int nodeID;

    public  SynchronizePacket(int nodeID) {
        this.packetType = PaxosPacketType.SYNC_REQUEST;
        this.nodeID = nodeID;

    }

    String NODE = "x1";
    public  SynchronizePacket(JSONObject jsonObject) throws JSONException{

        this.packetType = PaxosPacketType.SYNC_REQUEST;
        this.nodeID = jsonObject.getInt(NODE);

    }
    
	public int getType() {
		return this.packetType;
	}


    @Override
    public JSONObject toJSONObject() throws JSONException {
        JSONObject json = new JSONObject();
        json.put(PaxosPacketType.ptype, this.packetType);
        Packet.putPacketType(json, PacketType.PAXOS_PACKET); json.put(PaxosPacket.paxosIDKey, this.paxosID);

        json.put(NODE, nodeID);
        return json;
    }
}
