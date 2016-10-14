package edu.umass.cs.gnsserver.activecode.prototype.unblockingworker;

import java.io.IOException;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage;
import edu.umass.cs.gnsserver.activecode.prototype.ActiveMessage.Type;
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
	
	private Monitor monitor;
	
	/**
	 * @param channel
	 * @param ttl 
	 * @param guid 
	 */
	public ActiveQuerier(Channel channel, int ttl, String guid){
		this.channel = channel;
		this.currentTTL = ttl;
		this.currentGuid = guid;
		
		monitor = new Monitor();
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
   * @param id
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
			synchronized(monitor){
				while(!monitor.getDone()){				
					try {
						channel.sendMessage(am);
						monitor.wait();
					} catch (InterruptedException e) {
						//e.printStackTrace();
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

	private void writeValueIntoField(String querierGuid, String queriedGuid, String field, ValuesMap value, int ttl)
			throws ActiveException {
		
			ActiveMessage am = new ActiveMessage(ttl, querierGuid, field, queriedGuid, value, currentID);
			try {
				
				while(!monitor.getDone()){
					synchronized(monitor){
						try {
							channel.sendMessage(am);
							monitor.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
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
	
  /**
   *
   * @param response
   * @param isDone
   */
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
			this.response = response;
			this.isDone = isDone;	
			notifyAll();
		}
		
		ActiveMessage getResult(){
			return response;
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
