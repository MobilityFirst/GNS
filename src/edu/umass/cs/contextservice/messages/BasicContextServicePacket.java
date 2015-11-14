package edu.umass.cs.contextservice.messages;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * @author adipc
 *
 * @param <NodeIDType>
 */
public abstract class BasicContextServicePacket<NodeIDType> extends ContextServicePacket<NodeIDType>
{
	public BasicContextServicePacket(NodeIDType initiator, PacketType t) 
	{
		super(initiator);
		this.setType(t);	
	}
	
	public BasicContextServicePacket(JSONObject json) throws JSONException 
	{
		super(json);
	}
	
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = super.toJSONObjectImpl();
		return json;
	}

	public static void main(String[] args) 
	{
	}
}