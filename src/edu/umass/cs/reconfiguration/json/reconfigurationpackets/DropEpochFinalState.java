package edu.umass.cs.reconfiguration.json.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.Stringifiable;

/**
@author V. Arun
 */
public class DropEpochFinalState<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {

	private static enum Keys {DELETE_NAME};
	
	private final boolean deleteName; // will completely delete the name as opposed to a specific epoch
	
	public DropEpochFinalState(NodeIDType initiator, String name, int epochNumber, boolean deleteName) {
		super(initiator, ReconfigurationPacket.PacketType.DROP_EPOCH_FINAL_STATE, name, epochNumber);
		this.deleteName = deleteName;
	}
	public DropEpochFinalState(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.deleteName = (json.has(Keys.DELETE_NAME.toString()) ? json.getBoolean(Keys.DELETE_NAME.toString()) : false);
	}
	public boolean isDeleteName() {return this.deleteName;}
}
