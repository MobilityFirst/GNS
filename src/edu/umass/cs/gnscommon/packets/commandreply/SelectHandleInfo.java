package edu.umass.cs.gnscommon.packets.commandreply;

import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * This class represents a select handle that is returned after issuing  
 * a selectAndNotify GNSCommand. The select handle can be used to 
 * query the status of notifications. 
 * 
 * @author ayadav
 */
public class SelectHandleInfo
{	
	private final List<LocalSelectHandleInfo> localHandlesList;
	
	/**
	 * The constructor to create an object of this class. The constructor 
	 * requires a list of {@link LocalSelectHandleInfo}. 
	 * Each {@link LocalSelectHandleInfo} is a local select handle at the name
	 * server that processed the corresponding selectAndNotify GNSCommand.
	 * 
	 * @param localHandlesList
	 */
	public SelectHandleInfo(List<LocalSelectHandleInfo> localHandlesList)
	{
		this.localHandlesList = localHandlesList;
	}
	
	/**
	 * To serialize the object of this class into a JSONArray. 
	 * @return The serialized JSONArray representation of this object. 
	 * @throws JSONException
	 */
	public JSONArray toJSONArray() throws JSONException
	{
		JSONArray jsonArr = new JSONArray();
		for(int i=0; i<localHandlesList.size(); i++)
		{
			jsonArr.put(localHandlesList.get(i).toJSONObject());
		}
		return jsonArr;
	}
	
	/**
	 * To construct an object of this class from the supplied JSONArray. 
	 * 
	 * @param jsonArray
	 * @return The object of this class after de-serializing the supplied JSONArray. 
	 * @throws JSONException
	 */
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
	 * @return The list of local handles. 
	 */
	public List<LocalSelectHandleInfo> getLocalHandlesList()
	{
		return this.localHandlesList;
	}
}