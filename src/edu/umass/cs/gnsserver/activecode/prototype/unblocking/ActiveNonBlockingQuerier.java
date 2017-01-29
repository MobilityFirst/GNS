package edu.umass.cs.gnsserver.activecode.prototype.unblocking;

import java.io.IOException;
import java.net.InetAddress;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.record.Location;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.ACLQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.DNSQuerier;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import jdk.nashorn.api.scripting.ScriptObjectMirror;


public class ActiveNonBlockingQuerier implements Querier,ACLQuerier,DNSQuerier {
	private final Channel channel;
	private final DatabaseReader dbReader;
	private final ScriptObjectMirror JSON;
	private int currentTTL;
	private final String currentGuid;
	private final long currentID;
	
	private Monitor monitor;
	

	public ActiveNonBlockingQuerier(Channel channel, DatabaseReader dbReader, ScriptObjectMirror JSON, int ttl, String guid, long id){
		this.channel = channel;
		this.dbReader = dbReader;
		this.JSON = JSON;
		this.currentTTL = ttl;
		this.currentGuid = guid;
		this.currentID = id;
		
		monitor = new Monitor();
	}
	
	

	@Override
	public ScriptObjectMirror readGuid(String queriedGuid, String field) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			return string2JS(readValueFromField(currentGuid, currentGuid, field, currentTTL));
		return string2JS(readValueFromField(currentGuid, queriedGuid, field, currentTTL));
	}
	

	@Override
	public void writeGuid(String queriedGuid, String field, ScriptObjectMirror value) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			writeValueIntoField(currentGuid, currentGuid, field, js2String(value), currentTTL);
		else
			writeValueIntoField(currentGuid, queriedGuid, field, js2String(value), currentTTL);
	}
	
	@Override
	public JSONObject lookupUsernameForGuid(String targetGuid) throws ActiveException {
		throw new RuntimeException("unimplemented");
	}
	
	private String readValueFromField(String querierGuid, String queriedGuid, String field, int ttl)
			throws ActiveException {
		String value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid, currentID);
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

	private void writeValueIntoField(String querierGuid, String queriedGuid, String field, String value, int ttl)
			throws ActiveException {		
		ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid, value, currentID);			
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
	

  protected void release(ActiveMessage response, boolean isDone){
		monitor.setResult(response, isDone);
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
		
		synchronized void setResult(ActiveMessage response, boolean isDone){
			assert(response.type == Type.RESPONSE):"This is not a response!";
			this.isDone = isDone;
			this.response = response;				
			notifyAll();
		}
		
		ActiveMessage getResult(){
			return response;
		}
	}



	@Override
	public ScriptObjectMirror getLocations(ScriptObjectMirror ipList) throws ActiveException {
		// convert ipList to a JSONArray
		JSONArray arr = null;
		try {
			arr = new JSONArray(js2String(ipList));
		} catch (JSONException e) {
			throw new ActiveException("Array list can not be cast to a JSONArray");
		}
		
		// resolve ip one by one
		JSONObject obj = new JSONObject();
		for(int i=0; i<arr.length(); i++){
			try {
				String ip = arr.getString(i);
				Location loc = getLocation(ip);
				if(loc!=null){
					JSONObject value = new JSONObject();
					value.put("latitude", loc.getLatitude());
					value.put("longitude", loc.getLongitude());
					obj.put(ip, value);
				}
			} catch (JSONException e) {
				continue;
			}
		}
		return string2JS(obj.toString());

	}
	
	private Location getLocation(String ip) {		
		try {
			InetAddress ipAddress = InetAddress.getByName(ip);
			CityResponse response = dbReader.city(ipAddress);
			return response.getLocation();
			
		} catch (IOException | GeoIp2Exception e) {
			return null;
		}		
	}
	
	@SuppressWarnings("restriction")
	protected ScriptObjectMirror string2JS(String str){
		return (ScriptObjectMirror) JSON.callMember("parse", str);
	}
	
	@SuppressWarnings("restriction")
	protected String js2String(ScriptObjectMirror obj){
		return (String) JSON.callMember("stringify", obj);
	}
	

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
