package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.InterfaceRequest;
import edu.umass.cs.nio.IntegerPacketType;
import edu.umass.cs.nio.Stringifiable;
import edu.umass.cs.nio.StringifiableDefault;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;


/**
@author V. Arun
 * @param <NodeIDType> 
 */
public abstract class BasicReconfigurationPacket<NodeIDType> extends ReconfigurationPacket<NodeIDType> implements InterfaceRequest  {

	protected enum Keys {SERVICE_NAME, EPOCH_NUMBER, IS_COORDINATION};

	protected final String serviceName;
	protected final int epochNumber;

	/**
	 * @param initiator
	 * @param t
	 * @param name
	 * @param epochNumber
	 */
	public BasicReconfigurationPacket(NodeIDType initiator, PacketType t, String name, int epochNumber) {
		super(initiator);
		this.setType(t);
		this.serviceName = name;
		this.epochNumber = epochNumber;
	}
	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 */
	public BasicReconfigurationPacket(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.serviceName = json.getString(Keys.SERVICE_NAME.toString());
		this.epochNumber = json.getInt(Keys.EPOCH_NUMBER.toString());
	}
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.SERVICE_NAME.toString(), this.serviceName);
		json.put(Keys.EPOCH_NUMBER.toString(), this.epochNumber);
		return json;
	}

	public String getServiceName() {
		return this.serviceName;
	}
	/**
	 * @return Epoch number.
	 */
	public int getEpochNumber() {
		return this.epochNumber;
	}
	/**
	 * @return A pretty-print summary.
	 */
	public String getSummary() {
		return getType() + ":"+getServiceName() +":"+getEpochNumber();
	}
	
	public IntegerPacketType getRequestType() throws RequestParseException {
		return this.getType();
	}
	

	static void main(String[] args) {
		class BRP extends BasicReconfigurationPacket<Integer> {
			BRP(Integer initiator, PacketType t, String name, int epochNumber) {
				super(initiator, t, name, epochNumber);
			}
			BRP(JSONObject json) throws JSONException {
				super(json, new StringifiableDefault<Integer>(0));
			}
		}
		BRP brc = new BRP(3, ReconfigurationPacket.PacketType.DEMAND_REPORT, "name1", 4);
		System.out.println(brc);
		try {
			System.out.println(new BRP(brc.toJSONObject()));
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
