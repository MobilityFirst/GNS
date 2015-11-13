package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class defines the packet type of the GNS trigger
 * @author ayadav
 */

public class ValueUpdateFromGNS<NodeIDType> extends BasicContextServicePacket<NodeIDType>
{
	private enum Keys {VERSION_NUM, GUID, ATTR_VALUE_PAIR, USER_REQUESTID, SOURCE_IP, SOURCE_PORT};
	
	private final long versionNum;
	private final String GUID;
	private final JSONObject attrValuePair;
	private final long userRequestID;
	private final String sourceIP;
	private final int sourcePort;
	//private final String attrName;
	//private final String oldVal;
	//private final String newVal;
	//private final JSONObject allAttributes; // contains all context attributes for the group update trigger.
	//private final long updateStartTime;
	
	public ValueUpdateFromGNS( NodeIDType initiator, long versionNum, String GUID, JSONObject attrValuePair, 
			String sourceIP, int sourcePort, long userRequestID )
	{
		super(initiator, ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS);
		this.versionNum = versionNum;
		this.GUID = GUID;
		this.attrValuePair = attrValuePair;
		this.userRequestID = userRequestID;
		this.sourceIP = sourceIP;
		this.sourcePort = sourcePort;
	}
	
	public ValueUpdateFromGNS(JSONObject json) throws JSONException
	{
		//ValueUpdateFromGNS((NodeIDType)0, json.getString(Keys.GUID.toString()), 
		//		json.getDouble(Keys.OLDVALUE.toString()), json.getDouble(Keys.NEWVALUE.toString()));
		super(json);
		this.versionNum = json.getLong(Keys.VERSION_NUM.toString());
		this.GUID = json.getString(Keys.GUID.toString());
		this.attrValuePair = json.getJSONObject(Keys.ATTR_VALUE_PAIR.toString());
		this.userRequestID = json.getLong(Keys.USER_REQUESTID.toString());
		this.sourceIP = json.getString(Keys.SOURCE_IP.toString());
		this.sourcePort = json.getInt(Keys.SOURCE_PORT.toString());
		
		//this.oldVal = json.getString(Keys.OLDVALUE.toString());
		//this.newVal = json.getString(Keys.NEWVALUE.toString());
		//this.allAttributes = json.getJSONObject(Keys.ALL_OTHER_ATTRs.toString());
		//this.updateStartTime = json.getLong(Keys.UPDATE_START_TIME.toString());
		//System.out.println("\n\n ValueUpdateFromGNS constructor");
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException
	{
		JSONObject json = super.toJSONObjectImpl();
		json.put(Keys.VERSION_NUM.toString(), this.versionNum);
		json.put(Keys.GUID.toString(), this.GUID);
		json.put(Keys.ATTR_VALUE_PAIR.toString(), attrValuePair);
		json.put(Keys.SOURCE_IP.toString(), this.sourceIP);
		json.put(Keys.SOURCE_PORT.toString(), this.sourcePort);
		json.put(Keys.USER_REQUESTID.toString(), this.userRequestID);
		
		//json.put(Keys.OLDVALUE.toString(), this.oldVal);
		//json.put(Keys.NEWVALUE.toString(), this.newVal);
		//json.put(Keys.ALL_OTHER_ATTRs.toString(), this.allAttributes);
		
		//json.put(Keys.UPDATE_START_TIME.toString(), this.updateStartTime);
		return json;
	}
	
	public long getVersionNum()
	{
		return this.versionNum;
	}
	
	public String getGUID()
	{
		return GUID;
	}
	
	public JSONObject getAttrValuePairs()
	{
		return this.attrValuePair;
	}
	
	public String getSourceIP()
	{
		return this.sourceIP;
	}
	
	public int getSourcePort()
	{
		return this.sourcePort;
	}
	
	public long getUserRequestID()
	{
		return this.userRequestID;
	}
	
	/*public String getAttrName()
	{
		return attrName;
	}
	public String getNewVal()
	{
		return this.newVal;
	}*/
	public static void main(String[] args)
	{
	}
}