package edu.umass.cs.gnsserver.activecode.prototype.blocking;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.DNSQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.activecode.prototype.utils.GeoIPUtils;
import jdk.nashorn.api.scripting.ScriptObjectMirror;

/**
 * This class is an implementation of Querier, Querier only contains
 * readGuid and writeGuid method, so the protected methods will not be
 * exposed to the javascript code.
 * @author gaozy
 * 
 */
public class ActiveBlockingQuerier implements Querier,DNSQuerier {
	
	private final Channel channel;
	private final DatabaseReader dbReader;
	private final ScriptObjectMirror JSON;
	private int currentTTL;
	private final String currentGuid;
	private final long currentID;
	
	/**
	 * @param channel
	 * @param dbReader 
	 * @param JSON 
	 * @param ttl 
	 * @param guid 
	 * @param id 
	 */
	public ActiveBlockingQuerier(Channel channel, DatabaseReader dbReader, ScriptObjectMirror JSON, int ttl, String guid, long id){
		this.channel = channel;
		this.dbReader = dbReader;
		this.JSON = JSON;
		this.currentTTL = ttl;
		this.currentGuid = guid;
		this.currentID = id;
	}
	
	
	/**
	 * @param channel
	 * @param JSON 
	 */
	public ActiveBlockingQuerier(Channel channel, ScriptObjectMirror JSON){
		this(channel, null, JSON, 0, null, 0);
	}

	
	/**
	 * @param queriedGuid
	 * @param fields
	 * @return ValuesMap the code trying to read
	 * @throws ActiveException
	 */
	@Override
	public ScriptObjectMirror readGuid(ScriptObjectMirror fields, String queriedGuid) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		String queriedFields = js2String(fields);
		if(queriedGuid==null)
			return readValueFromField(currentGuid, currentGuid, queriedFields, currentTTL);
		return readValueFromField(currentGuid, queriedGuid, queriedFields, currentTTL);
	}
	
	/**
	 * @param queriedGuid
	 * @param value
	 * @throws ActiveException
	 */
	@Override
	public void writeGuid(ScriptObjectMirror value, String queriedGuid) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			writeValueIntoField(currentGuid, currentGuid, js2String(value), currentTTL);
		else
			writeValueIntoField(currentGuid, queriedGuid, js2String(value), currentTTL);
	}
	
	/**
	 * 
	 * @param querierGuid
	 * @param queriedGuid
	 * @param fields a JS Array is stringified to this string
	 * @param ttl
	 * @return
	 * @throws ActiveException
	 */
	private ScriptObjectMirror readValueFromField(String querierGuid, String queriedGuid, String fields, int ttl)
			throws ActiveException {
		ScriptObjectMirror value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, fields, queriedGuid, currentID);
			channel.sendMessage(am);
			ActiveMessage response = (ActiveMessage) channel.receiveMessage();
			
			if(response == null){
				throw new ActiveException();
			}
			
			if (response.getError() != null){
				throw new ActiveException();
			}
			value = string2JS(response.getValue());
		} catch(IOException e) {
			throw new ActiveException();
		}
		return value;
	}

	private void writeValueIntoField(String querierGuid, String targetGuid, String value, int ttl)
			throws ActiveException {
		
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, null, targetGuid, value, currentID);
			try {
				channel.sendMessage(am);
				ActiveMessage response = (ActiveMessage) channel.receiveMessage();
				
				if(response == null){
					throw new ActiveException();
				}
				if (response.getError() != null){
					throw new ActiveException();
				}
			} catch (IOException e) {
				throw new ActiveException();
			}
	}


	@Override
	public ScriptObjectMirror getLocations(ScriptObjectMirror ipList) throws ActiveException {
		// convert ipList to a JSONArray
		JSONArray arr = null;
		try {
			arr = new JSONArray("["+ipList.callMember("toString")+"]");
		} catch (JSONException e) {
			e.printStackTrace();
			throw new ActiveException(e.getMessage());
		}
		
		// resolve ip one by one
		JSONObject obj = new JSONObject();
		for(int i=0; i<arr.length(); i++){
			try {
				String ip = arr.getString(i);
				CityResponse loc = GeoIPUtils.getLocation_City(ip, dbReader);
				if(loc!=null){
					JSONObject value = new JSONObject();
					value.put("latitude", loc.getLocation().getLatitude());
					value.put("longitude", loc.getLocation().getLongitude());
					// continent of the location
					value.put("continent", loc.getContinent().getCode());
					obj.put(ip, value);
				}
			} catch (JSONException e) {
				continue;
			}
		}
		
		return string2JS(obj.toString());
	}
	
	protected ScriptObjectMirror string2JS(String str){
		return (ScriptObjectMirror) JSON.callMember("parse", str);
	}
	
	protected String js2String(ScriptObjectMirror obj){
		return (String) JSON.callMember("stringify", obj);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){		
		
		int n = 1000000;		
		long t1 = System.currentTimeMillis();		
		for(int i=0; i<n; i++){
			
		}		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}

}
