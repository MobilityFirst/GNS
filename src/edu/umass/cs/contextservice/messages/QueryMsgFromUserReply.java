package edu.umass.cs.contextservice.messages;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class QueryMsgFromUserReply<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {QUERY, QUERY_GUID, GUIDs, USER_REQ_NUM, REPLY_SIZE};
	
	private final String query;  // original query sent by the user.
	private final String queryGUID;
	private final JSONArray resultGUIDs;
	private final long userReqNum;
	private final int replySize;
	
	public QueryMsgFromUserReply(NodeIDType initiator, String query, String queryGUID, JSONArray resultGUIDs
			, long userReqNum, int replySize)
	{
		super(initiator, ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY);
		
		this.resultGUIDs = resultGUIDs;
		
		this.query = query;
		this.queryGUID = queryGUID;
		this.userReqNum = userReqNum;
		this.replySize = replySize;
	}
	
	public QueryMsgFromUserReply(JSONObject json) throws JSONException
	{
		super(json);
		
		this.resultGUIDs = json.getJSONArray(Keys.GUIDs.toString());
		this.query = json.getString(Keys.QUERY.toString());
		this.userReqNum = json.getLong(Keys.USER_REQ_NUM.toString());
		this.queryGUID = json.getString(Keys.QUERY_GUID.toString());
		this.replySize = json.getInt(Keys.REPLY_SIZE.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GUIDs.toString(), resultGUIDs);
		json.put(Keys.QUERY.toString(), query);
		json.put(Keys.USER_REQ_NUM.toString(), this.userReqNum);
		json.put(Keys.QUERY_GUID.toString(), this.queryGUID);
		json.put(Keys.REPLY_SIZE.toString(), this.replySize);
		return json;
	}
	
	public JSONArray getResultGUIDs()
	{
		return this.resultGUIDs;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public long getUserReqNum()
	{
		return this.userReqNum;
	}
	
	public String getQueryGUID()
	{
		return this.queryGUID;
	}
	
	public int getReplySize()
	{
		return this.replySize;
	}
	
	public static void main(String[] args)
	{
	}
}