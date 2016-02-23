/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Misha Badov, Westy
 *
 */
package edu.umass.cs.gnsserver.activecode;

import java.io.IOException;
import java.net.DatagramSocket;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is used to communicate with active worker.
 * It sends active code to the worker, receives request
 * from the worker, and gets the execution result from
 * the worker.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeClient {
//	protected Lock lock = new ReentrantLock();
	private boolean ready = false;
	private int serverPort;
	private Process process;
	private final GnsApplicationInterface<String> app;
	private DatagramSocket clientSocket;
	private byte[] buffer = new byte[8096*10];
	private ActiveCodeHandler ach;
	
	
	/**
	 * @param app the gns app
	 * @param ach 
     * @param port 
	 * @param proc 
	 */
	public ActiveCodeClient(GnsApplicationInterface<String> app, ActiveCodeHandler ach, int port, Process proc) {
		this.app = app;
		this.ach = ach;
		this.ready = false;
		//initialize the clientSocket first
		try{
			clientSocket = new DatagramSocket();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		setNewWorker(port, proc);
	}
	
	protected int getPort(){
		return serverPort;
	}
	  
	
	/**
	 * 
	 * @param acp the parameters to send to the worker
	 * @return the ValuesMap object returned by the active code
	 */
	public ValuesMap runActiveCode(ActiveCodeParams acp) {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setAcp(acp);
		ValuesMap vm = null;
		
		vm = submitRequest(acm, ach);
		
		return vm;
	}
	
	/**
	 * Checks to see if the worker is still running.
	 * @return true if the worker is still running
	 */
	protected boolean isRunning() {
		try {
			process.exitValue();
		}
		catch (IllegalThreadStateException e) {
			return true;
		}
		return false;
	}
	
	/**
	 * Submits the request to the worker via socket comm.
	 * @param acmReq
	 * @return the ValuesMap object returned by the active code
	 */
	protected ValuesMap submitRequest(ActiveCodeMessage acmReq, ActiveCodeHandler ach) {
		long startTime = System.nanoTime();
		boolean crashed = false;
		
		ActiveCodeQueryHelper acqh = new ActiveCodeQueryHelper(app, ach);
		
		boolean codeFinished = false;
		String valuesMapString = null;
		
		// Serialize the request
		ActiveCodeUtils.sendMessage(this.clientSocket, acmReq, serverPort);
		DelayProfiler.updateDelayNano("activeSendMessage", startTime);
				
		long receivedTime = System.nanoTime();
		// Keeping going until we have received a 'finished' message
		while(!codeFinished) {
		    ActiveCodeMessage acmResp = ActiveCodeUtils.receiveMessage(clientSocket, this.buffer);
		    
		    if(acmResp.isFinished()) {
		    	// We are done!		
		    	codeFinished = true;
		    	valuesMapString = acmResp.getValuesMapString();
		    	crashed = acmResp.isCrashed();
		    }
		    else {
		    	// We aren't finished, which means that the response asked us to query a guid
		    	// Can be read or write query (writes are only supported locally)
		    	String currentGuid = acmReq.getAcp().getGuid();
		    	ActiveCodeQueryRequest acqreq = acmResp.getAcqreq();
		    	// Perform the query
		    	ActiveCodeQueryResponse acqresp = acqh.handleQuery(currentGuid, acqreq);
		    	// Send the results back
		    	ActiveCodeMessage acmres = new ActiveCodeMessage();
		    	acmres.setAcqresp(acqresp);
		    	ActiveCodeUtils.sendMessage(clientSocket, acmres, serverPort);
		    }
		}

		DelayProfiler.updateDelayNano("activeReceiveMessage", receivedTime);
		
		long convertTime = System.nanoTime();
		ValuesMap vm = null;
		
        // Try to convert back to a valuesMap
        if(crashed) {
        	System.out.println("################### "+ acmReq.getAcp().getGuid()+" Crashed! #################### "+acmReq.getValuesMapString());
        	try{
        		//If there is an error, send the original value back
        		vm = new ValuesMap(new JSONObject(acmReq.getValuesMapString()));
        	} catch (JSONException e) {
        		e.printStackTrace();
        	}
        	return vm;
        }else if(valuesMapString != null) {        	
        	try {
        		vm = new ValuesMap(new JSONObject(valuesMapString));
 	        } catch (JSONException e) {
 	        	e.printStackTrace();
 	        }
        }else{    
        	System.out.println("ValuesMapString is "+acmReq.toString());
        }
        DelayProfiler.updateDelayNano("activeConvert", convertTime);
        DelayProfiler.updateDelayNano("communication", startTime);
        return vm;
	}
	
	/**
	 * Cleanly shuts down the worker
	 */
	public void shutdownServer() {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setShutdown(true);
		try {
			submitRequest(acm, ach);
		} catch (Exception e) {
			e.printStackTrace();
		}
		clientSocket.close();
	}
	
	protected boolean isReady(){
		return ready;
	}
	
	protected void setReady(boolean ready){
		this.ready = ready;
	}
	
	protected void forceShutdownServer() {
		process.destroyForcibly();
		clientSocket.close();
		try{
			clientSocket = new DatagramSocket();
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	protected void setNewWorker(int port, Process proc){
		this.process = proc;
		this.serverPort = port;
		
		notifyWorkerOfClientPort(port);
	}
	
	protected void notifyWorkerOfClientPort(int port){
		//notify new worker about the client's new port number
		ActiveCodeUtils.sendMessage(clientSocket, new ActiveCodeMessage(), port);
	}
}
