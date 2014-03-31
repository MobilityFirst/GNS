package edu.umass.cs.gns.nsdesign.packet;

import edu.umass.cs.gns.nsdesign.packet.Packet.PacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashSet;
import java.util.Set;

/**
 * This packet is exchanged among replica controllers to propose a new set of actives among primary name servers.
 *
 * This packet is created by a replica controller that wants to propose a new set of actives. The packet is
 * then forwarded to the appropriate set of replica controllers. After being committed by active replicas,
 * each replica controller updates its database with the new set of proposed actives.
 *
 * @author abhigyan
 *
 */
public class NewActiveProposalPacket extends BasicPacket {

	private final static String NAME = "name";

	private final static String PROPOSING_NODE = "propNode";

	private final static String NEW_ACTIVES = "actives";

	private final static String VERSION = "version";

	/**
	 * name for which the new actives are being proposed
	 */
	String name;

	/**
	 * node which proposed this message.
	 */
	int proposingNode;

	/**
	 * current set of actives of this node.
	 */
	Set<Integer> newActives;

	/**
	 * Verion number of this new set of active name servers.
	 */
	int version;

	/**
	 * Constructor method
	 * @param name  name for which the new actives are being proposed
	 * @param proposingNode  node which proposed this message.
	 * @param newActives  current set of actives of this node.
   * @param version Version number for this new set of active name servers.
	 */
	public NewActiveProposalPacket(String name, int proposingNode, Set<Integer> newActives, int version) {
		this.type = PacketType.NEW_ACTIVE_PROPOSE;
		this.name = name;
		this.proposingNode = proposingNode;
		this.newActives = newActives;
		this.version = version;
	}


	public NewActiveProposalPacket(JSONObject json) throws JSONException {

		this.type = Packet.getPacketType(json);
		this.name = json.getString(NAME);

		this.proposingNode = json.getInt(PROPOSING_NODE);

		String actives = json.getString(NEW_ACTIVES);

		this.newActives = new HashSet<Integer>();

		String[] activeSplit = actives.split(":");

		for (String x: activeSplit) {
			newActives.add(Integer.parseInt(x));
		}

		this.version = json.getInt(VERSION);
	}

	/**
	 * JSON object that is implemented.
	 * @return
	 * @throws org.json.JSONException
	 */
	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, getType());
		json.put(NAME, name);
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
		json.put(VERSION, version);
		return json;
	}
	
	
	
	public String getName() {
		return name;
	}

	
	public int getProposingNode() {
		return proposingNode;
	}
	
	public Set<Integer> getProposedActiveNameServers() {
		return newActives;
	}
	
	public int getVersion() {
		return version;
	}
	
}
