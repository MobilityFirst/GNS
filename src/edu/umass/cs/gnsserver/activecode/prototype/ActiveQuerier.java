package edu.umass.cs.gnsserver.activecode.prototype;

import java.io.IOException;

import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Channel;
import edu.umass.cs.gnsserver.activecode.prototype.interfaces.Querier;
import edu.umass.cs.gnsserver.utils.ValuesMap;

/**
 * This class is an implementation of Querier, Querier only contains
 * readGuid and writeGuid method, so the protected methods will not be
 * exposed to the javascript code.
 * @author gaozy
 *
 */
public class ActiveQuerier implements Querier {
	private Channel channel;
	private int currentTTL;
	private String currentGuid;
	private long currentID;
	
	/**
	 * @param channel
	 * @param ttl 
	 * @param guid 
	 */
	public ActiveQuerier(Channel channel, int ttl, String guid){
		this.channel = channel;
		this.currentTTL = ttl;
		this.currentGuid = guid;
	}
	
	
	/**
	 * @param channel
	 */
	public ActiveQuerier(Channel channel){
		this(channel, 0, null);
	}
	
	/**
	 * @param guid
	 * @param ttl
	 */
	protected void resetQuerier(String guid, int ttl, long id){
		this.currentGuid = guid;
		this.currentTTL = ttl;
		this.currentID = id;
	}
	
	/**
	 * @param queriedGuid
	 * @param field
	 * @return ValuesMap the code trying to read
	 * @throws ActiveException
	 */
	@Override
	public ValuesMap readGuid(String queriedGuid, String field) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			return readValueFromField(currentGuid, currentGuid, field, currentTTL--);
		return readValueFromField(currentGuid, queriedGuid, field, currentTTL--);
	}
	
	/**
	 * @param queriedGuid
	 * @param field
	 * @param value
	 * @throws ActiveException
	 */
	@Override
	public void writeGuid(String queriedGuid, String field, ValuesMap value) throws ActiveException{
		if(currentTTL <=0)
			throw new ActiveException(); //"Out of query limit"
		if(queriedGuid==null)
			writeValueIntoField(currentGuid, currentGuid, field, value, currentTTL--);
		else
			writeValueIntoField(currentGuid, queriedGuid, field, value, currentTTL--);
	}
	
	
	private ValuesMap readValueFromField(String querierGuid, String queriedGuid, String field, int ttl)
			throws ActiveException {
		ValuesMap value = null;
		try{
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid, currentID);
			channel.sendMessage(am);
			ActiveMessage response = (ActiveMessage) channel.receiveMessage();
			value = response.getValue();
		} catch(IOException e) {
			throw new ActiveException();
		}
		return value;
	}

	private void writeValueIntoField(String querierGuid, String queriedGuid, String field, ValuesMap value, int ttl)
			throws ActiveException {
		
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid, value, currentID);
			try {
				//System.out.println("Querier sends request "+am);
				channel.sendMessage(am);
				ActiveMessage response = (ActiveMessage) channel.receiveMessage();
				//System.out.println("The response is "+response);
				if (response.getError() != null){
					throw new ActiveException();
				}
			} catch (IOException e) {
				throw new ActiveException();
			}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args){
		ActiveQuerier querier = new ActiveQuerier(null);
		int ttl = 1;
		String guid = "Zhaoyu Gao";			
		
		int n = 1000000;		
		long t1 = System.currentTimeMillis();		
		for(int i=0; i<n; i++){
			querier.resetQuerier(guid, ttl, 0);
		}		
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms, and the average latency for each operation is "+(elapsed*1000.0/n)+"us");
	}
}
