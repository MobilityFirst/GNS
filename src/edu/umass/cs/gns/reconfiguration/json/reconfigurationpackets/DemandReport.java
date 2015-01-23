package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.nio.IntegerPacketType;
import edu.umass.cs.gns.reconfiguration.InterfaceReplicableRequest;
import edu.umass.cs.gns.reconfiguration.RequestParseException;
import edu.umass.cs.gns.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.gns.util.Stringifiable;
import edu.umass.cs.gns.util.StringifiableDefault;
import edu.umass.cs.gns.util.Util;


/**
@author V. Arun
 */
public class DemandReport<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> implements InterfaceReplicableRequest {
	public enum Keys {STATS};

	private final JSONObject stats;
	//private boolean coordType = false;

	public DemandReport(NodeIDType initiator, String name, int epochNumber, JSONObject stats) {
		super(initiator, ReconfigurationPacket.PacketType.DEMAND_REPORT, name, epochNumber);
		this.stats = stats;
	}
	public DemandReport(NodeIDType initiator, String name, int epochNumber, AbstractDemandProfile demand) {
		super(initiator, ReconfigurationPacket.PacketType.DEMAND_REPORT, name, epochNumber);
		this.stats = demand.getStats();
	}
	public DemandReport(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
		super(json, unstringer);
		this.stats = json.getJSONObject(Keys.STATS.toString());
	}
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.STATS.toString(), this.stats);
		return json;
	}
	public JSONObject getStats() {
		return this.stats;
	}
	@Override
	public IntegerPacketType getRequestType() throws RequestParseException {
		return ReconfigurationPacket.PacketType.DEMAND_REPORT;
	}
	@Override
	public boolean needsCoordination() {
		return false;
		//return coordType;
	}
	@Override
	public void setNeedsCoordination(boolean b) {
		//coordType = b;
	}
	
	public static void main(String[] args) {
		JSONObject stats = new JSONObject();
		try {
			Util.assertAssertionsEnabled();
			stats.put("rate", 0.33);
			stats.put("numRequests", 24);
			stats.put("numTotalRequests", 24);

			DemandReport<Integer> dr = new DemandReport<Integer>(4, "name1", 2, stats);
			System.out.println(dr);
			DemandReport<Integer> dr2 = new DemandReport<Integer>(dr.toJSONObject(), new StringifiableDefault<Integer>(0));
			System.out.println(dr2);
			assert(dr.toString().length()==dr2.toString().length());
			assert(dr.toString().indexOf("}") == dr2.toString().indexOf("}"));
			assert(dr.toString().equals(dr2.toString())) : dr.toString() + "!=" + dr2.toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
