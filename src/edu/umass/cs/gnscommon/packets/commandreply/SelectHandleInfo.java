package edu.umass.cs.gnscommon.packets.commandreply;

import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * This class represents a handle for select notifications. 
 * 
 * @author ayadav
 *
 */
public class SelectHandleInfo
{	
	private final List<LocalSelectHandleInfo> localHandlesList;
	
	public SelectHandleInfo(List<LocalSelectHandleInfo> localHandlesList)
	{
		this.localHandlesList = localHandlesList;
	}
	
	
	public JSONArray toJSONArray() throws JSONException
	{
		JSONArray jsonArr = new JSONArray();
		for(int i=0; i<localHandlesList.size(); i++)
		{
			jsonArr.put(localHandlesList.get(i).toJSONObject());
		}
		return jsonArr;
	}
	
	
	public static SelectHandleInfo fromJSONArray(JSONArray jsonArray) throws JSONException
	{
		List<LocalSelectHandleInfo> localHandlesList 
									= new LinkedList<LocalSelectHandleInfo>();
		for(int i=0; i<jsonArray.length(); i++)
		{
			LocalSelectHandleInfo local = LocalSelectHandleInfo.fromJSONObject
																(jsonArray.getJSONObject(i));
			
			localHandlesList.add(local);
		}	
		return new SelectHandleInfo(localHandlesList);
	}
	
	
	/**
	 * Returns the lsit of local handles corresponding to 
	 * this select handle. 
	 * 
	 * @return
	 */
	public List<LocalSelectHandleInfo> getLocalHandlesList()
	{
		return this.localHandlesList;
	}
}