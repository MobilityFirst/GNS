package edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets;


import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.reconfiguration.reconfigurationutils.DemandProfile;


/**
@author V. Arun
 */
public class DemandReport<NodeIDType> extends BasicReconfigurationPacket<NodeIDType> {
	public enum Keys {STATS, RATE, NUM_REQUESTS, NUM_TOTAL_REQUESTS};

	private final JSONObject stats;

	public DemandReport(NodeIDType initiator, String name, int epochNumber, JSONObject stats) {
		super(initiator, ReconfigurationPacket.PacketType.DEMAND_REPORT, name, epochNumber);
		this.stats = stats;
	}
	public DemandReport(NodeIDType initiator, String name, int epochNumber, DemandProfile demand) {
		super(initiator, ReconfigurationPacket.PacketType.DEMAND_REPORT, name, epochNumber);
		this.stats = getStats(demand);
	}
	public DemandReport(JSONObject json) throws JSONException {
		super(json);
		this.stats = json.getJSONObject(Keys.STATS.toString());
	}
	public JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.STATS.toString(), this.stats);
		return json;
	}
	private static JSONObject getStats(DemandProfile demand) {
		JSONObject json = new JSONObject();
		try {
			json.put(DemandReport.Keys.RATE.toString(), demand.getRequestRate());
			json.put(DemandReport.Keys.NUM_REQUESTS.toString(), demand.getNumRequests());
			json.put(DemandReport.Keys.NUM_TOTAL_REQUESTS.toString(), demand.getNumTotalRequests());			
		} catch(JSONException je) {
			je.printStackTrace();
		}
		return json;
	}
	public static void main(String[] args) {
		JSONObject stats = new JSONObject();
		try {
			stats.put("rate", 0.33);
			stats.put("numRequests", 24);
			stats.put("numTotalRequests", 24);

			DemandReport<Integer> dr = new DemandReport<Integer>(4, "name1", 2, stats);
			System.out.println(dr);
			DemandReport<Integer> dr2 = new DemandReport<Integer>(dr.toJSONObject());
			System.out.println(dr2);
			assert(dr.toString().length()==dr2.toString().length());
			assert(dr.toString().indexOf("}") == dr2.toString().indexOf("}"));
			assert(dr.toString().equals(dr2.toString())) : dr.toString() + "!=" + dr2.toString();
		} catch(JSONException je) {
			je.printStackTrace();
		}
	}
}
