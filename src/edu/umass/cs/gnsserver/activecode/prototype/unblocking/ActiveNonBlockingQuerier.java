package edu.umass.cs.gnsserver.activecode.prototype.unblocking;

import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CityResponse;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
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
 */
public class ActiveNonBlockingQuerier implements Querier,DNSQuerier {
	private final Channel channel;
	private final DatabaseReader dbReader;
	private final ScriptObjectMirror JSON;
	private int currentTTL;
	private final String currentGuid;
	private final long currentID;
	private Monitor monitor;
	
	/**
	 * @param channel
	 * @param dbReader 
	 * @param JSON 
	 * @param ttl 
	 * @param guid 
	 * @param id 
	 */
	public ActiveNonBlockingQuerier(Channel channel, DatabaseReader dbReader, ScriptObjectMirror JSON, int ttl, String guid, long id){
		this.channel = channel;
		this.dbReader = dbReader;
		this.JSON = JSON;
		this.currentTTL = ttl;
		this.currentGuid = guid;
		this.currentID = id;
	}
	
	
	/**
	 * @param queriedGuid
	 * @param queriedFields
	 * @return ValuesMap the code trying to read
	 * @throws ActiveException
	 */
	@Override
	public ScriptObjectMirror readGuid(String queriedFields, String queriedGuid) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		//String queriedFields = js2String(field);		
		if(queriedGuid==null)
			return string2JS(readValueFromField(currentGuid, currentGuid, queriedFields, currentTTL));
		return string2JS(readValueFromField(currentGuid, queriedGuid, queriedFields, currentTTL));
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
	private String readValueFromField(String querierGuid, String queriedGuid, String fields, int ttl)
			throws ActiveException {
		monitor = new Monitor();
		String value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, fields, queriedGuid, currentID);
			channel.sendMessage(am);
			synchronized(monitor){
				while(!monitor.getDone()){				
					try {						
						monitor.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
						return null;
					}
				}
			}
					
			ActiveMessage response = monitor.getResult();
			
			if(response == null){
				throw new ActiveException();
			}
			
			if (response.getError() != null){
				throw new ActiveException();
			}
			value = response.getValue();
		} catch(IOException e) {
			throw new ActiveException();
		}
		
		return value;
	}

	private void writeValueIntoField(String querierGuid, String queriedGuid, String value, int ttl)
			throws ActiveException {
		monitor = new Monitor();
		ActiveMessage am = new ActiveMessage(ttl, querierGuid, null, queriedGuid, value, currentID);			
		try {
			channel.sendMessage(am);
			synchronized(monitor){
				while(!monitor.getDone()){					
					try {							
						monitor.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
						return;
					}
				}
			}
			
			ActiveMessage response = monitor.getResult();
			
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
	
	
	private static class Monitor {
		boolean isDone;
		ActiveMessage response;
		
		Monitor(){
			this.isDone = false;
		}
		
		boolean getDone(){
			return isDone;
		}
		
		protected synchronized void setResult(ActiveMessage response, boolean isDone){
			assert(response.type == Type.RESPONSE):"This is not a response!";
			this.isDone = isDone;
			this.response = response;				
			notifyAll();
		}
		
		ActiveMessage getResult(){
			return response;
		}
	}

	/**
     * @param response
	 * @param isDone
	 */
	protected void release(ActiveMessage response, boolean isDone){
		monitor.setResult(response, isDone);
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
		
		//System.out.println(">>>>>>>>>>>> The query is "+arr.toString());
		
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
		
		//System.out.println("The result is "+obj.toString());
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
		//ActiveNonBlockingQuerier querier = null;
		
		long t = System.currentTimeMillis();
		for(int i=0; i<n; i++){
			
		}
		long elapsed = System.currentTimeMillis() - t;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");		
	}

}
