package edu.umass.cs.gns.packet;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

public class TinyQuery  extends BasicPacket{

	String VAL = "val";

	int queryID;
	int LNSID;
	String name;
	Set<Integer> actives; 
	Set<Integer> primaries;

	public TinyQuery(int queryID, int LNSID, String name, 
			Set<Integer> actives, Set<Integer> primaries) {
		this.queryID = queryID;
		this.LNSID = LNSID;
		this.name = name;
		if (actives != null) this.actives = actives;
		else  actives = new HashSet<Integer>();
			
		if (primaries != null) this.primaries = primaries;
		else primaries = new HashSet<Integer>();
		
		type =  Packet.PacketType.TINY_QUERY;
	}

	public TinyQuery(JSONObject json) throws JSONException{

		type = Packet.PacketType.TINY_QUERY;
		
		String x = json.getString(VAL);
		
		String [] vals = x.split(":");
		this.queryID = Integer.parseInt(vals[0]);
		this.LNSID = Integer.parseInt(vals[1]);
		this.name = vals[2];
		this.actives = stringToHashSet(vals[3]);
		this.primaries = stringToHashSet(vals[4]);
	}
	

	@Override
	public JSONObject toJSONObject() throws JSONException {
		JSONObject json = new JSONObject();
		Packet.putPacketType(json, getType());
		json.put(VAL, queryID + ":" + LNSID + ":" + name + ":" + actives.toString() + ":" + primaries.toString());

		return json;
	}

	/// GET SET methods
	
	public String getName() {
		return name;
	}
	
	public int getLNSID() {
		return LNSID;
	}
	

	public int getQueryID() {
		return queryID;
	}

	public Set<Integer> getActives() {
		return actives;
	}

	public Set<Integer> getPrimaries() {
		return primaries;
	}
	

	public void setLNSID(int LNSID) {
		this.LNSID = LNSID;
	}
	
	public void updateActivesPrimaries(HashSet<Integer> actives, HashSet<Integer> primaries) {
		this.actives = actives;
		this.primaries = primaries;
		
	}
	
	
	
	private static HashSet<Integer> stringToHashSet(String s) {
		String s1 = s.trim();
		String s2 = s1.substring(1, s1.length() - 1 );
		if (s2.trim().length() == 0) return new HashSet<Integer>();
		String[] tokens  = s2.split(",");
		
		HashSet<Integer> h = new HashSet<Integer>();
		for (String t: tokens){
//			System.out.println(t);
			h.add(Integer.parseInt(t.trim()));
		}
		return h;
	}


	public static void main(String []args) {
		
		HashSet<Integer> x = new HashSet<Integer>();
		x.add(3);
		
		x.add(45);
		TinyQuery t = new TinyQuery(1,2,"3", x, x);
		
//		System.out.println(TinyQuery.stringToHashSet(x.toString()).toString());
		try {
			System.out.println(new TinyQuery(t.toJSONObject()).toJSONObject());
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//		JSONObject json = new JSONObject();
		//		json.put("c", x.toString());
	}
}
