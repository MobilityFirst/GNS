package edu.umass.cs.gns.packet;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nameserver.NameRecordKey;
import edu.umass.cs.gns.packet.Packet.PacketType;

/**
 * This packet is used to propose a new set of actives among primary name servers.
 * @author abhigyan
 *
 */
public class NewActiveProposalPacket extends BasicPacket{

	private final static String NAME = "name";
	
	//private final static String RECORDKEY = "recordKey";

	private final static String PROPOSING_NODE = "propNode";

	private final static String NEW_ACTIVES = "actives";
	
	private final static String PAXOS_ID = "paxosID";

	/**
	 * name for which the proposal is being done.
	 */
	String name;

//	/**
//	 * name record key 
//	 */
//	NameRecordKey recordKey;

	/**
	 * a unique ID to represent this proposal.
	 */
//	int uniqueId;
	
	/**
	 * node which proposed this message. 
	 */
	int proposingNode;


	/**
	 * current set of actives of this node.
	 */
	Set<Integer> newActives;

	/**
	 * Paxos ID for this new set of active name servers.
	 */
	String paxosID;
	
	/**
	 * 
	 * @param name
	 * @param proposingNode
	 * @param newActives
	 */
	public NewActiveProposalPacket(String name, int proposingNode, 
			Set<Integer> newActives, String paxosID) {
		//this.recordKey = recordKey;
		this.type = PacketType.NEW_ACTIVE_PROPOSE;
		this.name = name;
		this.proposingNode = proposingNode;
		this.newActives = newActives;
		this.paxosID = paxosID;
	}


	public NewActiveProposalPacket(JSONObject json) throws JSONException {
		
		this.type = Packet.getPacketType(json);
		this.name = json.getString(NAME);
//		this.uniqueId = json.getInt(UNIQUEID);
		//this.recordKey = NameRecordKey.valueOf(json.getString(RECORDKEY));
		
		this.proposingNode = json.getInt(PROPOSING_NODE);
		
		String actives = json.getString(NEW_ACTIVES);
		
		this.newActives = new HashSet<Integer>();
		
		String[] activeSplit = actives.split(":");
		
		for (String x: activeSplit) {
			newActives.add(Integer.parseInt(x));
		}
		
		this.paxosID = json.getString(PAXOS_ID);
	}

	/**
	 * JSON object that is implemented.
	 * @return
	 * @throws JSONException
	 */
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, getType());
		json.put(NAME, name);
		//json.put(RECORDKEY, recordKey.getName());
//		json.put(UNIQUEID, uniqueId);
		
		json.put(PROPOSING_NODE, proposingNode);
		
		// convert array to string
		StringBuilder sb = new StringBuilder();
		for (Integer x: newActives) {
			if (sb.length() == 0) 
				sb.append(x);
			else 
				sb.append(":" + x);
		}
		String actives = sb.toString();
		json.put(NEW_ACTIVES, actives);
		json.put(PAXOS_ID, paxosID);
		return json;
	}
	
	
	
	public String getName() {
		return name;
	}
	
	
//	public NameRecordKey getRecordKey() {
//		return recordKey;
//	}
	
	public int getProposingNode() {
		return proposingNode;
	}
	
	public Set<Integer> getProposedActiveNameServers() {
		return newActives;
	}
	
	public String getPaxosID() {
		return paxosID;
	}
	
}
