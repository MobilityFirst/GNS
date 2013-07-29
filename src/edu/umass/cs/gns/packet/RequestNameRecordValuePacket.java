package edu.umass.cs.gns.packet;

import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nameserver.NameRecordKey;

public class RequestNameRecordValuePacket extends BasicPacket
{
	// following fields are needed: name, recordkey, oldpaxosid, sentActiveID 
	

	private final static String NAME = "name";
	
	private final static String RECORDKEY = "recordKey";

	private final static String PRIMARY_SENDER = "primarySender";
	
	private final static String ACTIVE_SENDER = "activeSender";

	private final static String NEW_ACTIVES = "newActives";
	
	private final static String OLD_ACTIVES = "oldActives";
	
	private final static String NEW_ACTIVE_PAXOS_ID = "newPaxosID";
	
	private final static String OLD_ACTIVE_PAXOS_ID = "oldPaxosID";
	
	/**
	 * A unique ID to distinguish this packet at active replica 
	 */
	int uniqueID;
	
	/**
	 * name for which the proposal is being done.
	 */
	String name;

	/**
	 * name record key 
	 */
	NameRecordKey recordKey;
	
	/**
	 * primary node that sent this message 
	 */
	int primarySender;
	
	/**
	 * active node to which this message was sent
	 */
	int activeSender;
	
	/**
	 * current set of actives of this node.
	 */
	Set<Integer> newActives;

	/**
	 * Previous set of actives of this node.
	 */
	Set<Integer> oldActives;
	
	/**
	 * Paxos ID of the new set of actives. 
	 */
	String newActivePaxosID;
	

	/**
	 * Paxos ID of the old set of actives.
	 */
	String oldActivePaxosID;
	
	
	@Override
	public JSONObject toJSONObject() throws JSONException
	{
		
		return null;
	}

}
