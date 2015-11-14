package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

public class RefreshTrigger<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	// 1 for add, 2 for remove
	
	public static final int ADD						= 1;
	public static final int REMOVE					= 2;
	
	private enum Keys {QUERY, GROUP_GUID, VERSION_NUM, GUID, ADD_REMOVE};
	
	private final String query;  // original query sent by the user.
	private final String groupGUID;
	private final long versionNum;
	private final String updateInGUID;
	private final int addRemove;
	
	public RefreshTrigger(NodeIDType initiator, String query, String groupGUID, long versionNum,
			String GUID, int addRemove)
	{
		super(initiator, ContextServicePacket.PacketType.REFRESH_TRIGGER);
		
		this.groupGUID = groupGUID;
		this.query = query;
		this.versionNum = versionNum;
		this.updateInGUID = GUID;
		this.addRemove = addRemove;
	}
	
	public RefreshTrigger(JSONObject json) throws JSONException
	{
		super(json);
		
		this.groupGUID = json.getString(Keys.GROUP_GUID.toString());
		this.query = json.getString(Keys.QUERY.toString());
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.updateInGUID = json.getString(Keys.GUID.toString());
		this.addRemove = json.getInt(Keys.ADD_REMOVE.toString());
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.GROUP_GUID.toString(), groupGUID);
		json.put(Keys.QUERY.toString(), query);
		json.put(Keys.VERSION_NUM.toString(), versionNum);
		json.put(Keys.GUID.toString(), updateInGUID);
		json.put(Keys.ADD_REMOVE.toString(), addRemove);
		return json;
	}
	
	public String getGroupGUID()
	{
		return this.groupGUID;
	}
	
	public String getQuery()
	{
		return query;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public String getUpdateGUID()
	{
		return this.updateInGUID;
	}
	
	public int getAddRemove()
	{
		return this.addRemove;
	}
	
	public static void main(String[] args)
	{
	}
}