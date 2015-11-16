package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class QueryMsgFromUser<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, SOURCE_IP, SOURCE_PORT, USER_REQ_NUM};
	
	private final String query;
	private final String sourceIP;
	private final int sourcePort;
	private final long userReqNum;
	
	public QueryMsgFromUser(NodeIDType initiator, String userQuery, String sourceIP, 
			int sourcePort, long userReqNum)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_FROM_USER);
		this.query = userQuery;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
		this.userReqNum = userReqNum;
	}
	
	public QueryMsgFromUser(JSONObject json) throws JSONException
	{
		super(json);
		this.query = json.getString(Keys.QUERY.toString());
		this.sourceIP = json.getString(Keys.SOURCE_IP.toString());
		this.sourcePort = json.getInt(Keys.SOURCE_PORT.toString());
		
		//this.sourceIP = json.getString(ContextServicePacket.SENDERADDRESS);
		//this.sourcePort = json.getInt(ContextServicePacket.SENDERPORT);
		
		this.userReqNum = json.getInt(Keys.USER_REQ_NUM.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.QUERY.toString(), query);
		json.put(Keys.SOURCE_IP.toString(), sourceIP);
		json.put(Keys.SOURCE_PORT.toString(), sourcePort);
		json.put(Keys.USER_REQ_NUM.toString(), userReqNum);
		return json;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public String getSourceIP()
	{
		return this.sourceIP;
	}
	
	public int getSourcePort()
	{
		return this.sourcePort;
	}
	
	public long getUserReqNum()
	{
		return this.userReqNum;
	}
	
	public static void main(String[] args)
	{
	}
}