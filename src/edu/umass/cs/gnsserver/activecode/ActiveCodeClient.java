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
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gnsserver.interfaces.ActiveDBInterface;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is used to communicate with active worker. It sends active code to
 * the worker, receives request from the worker, and gets the execution result
 * from the worker.
 * 
 * @author Zhaoyu Gao
 */
public class ActiveCodeClient {
	private boolean ready = false;

	private int workerPort;
	private Process process;
	private DatagramSocket clientSocket;
	// For instrument only
	int getClientPort(){
		return clientSocket.getLocalPort();
	}
	
	private byte[] buffer = new byte[2048];
	protected final int myID = getNextID();
	private final ActiveCodeQueryHelper acqh;
	
	private static int globalID = getNextID();
	private synchronized static int getNextID() {
		return globalID++;
	}

	/**
	 * @param app the gnsApp
	 * @param port
	 * @param proc
	 */
	public ActiveCodeClient(ActiveDBInterface app, int port, Process proc) {
		this.ready = false;
		this.acqh = new ActiveCodeQueryHelper(app);
		
		// initialize the clientSocket first
		try {
			clientSocket = new DatagramSocket();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		setNewWorker(port, proc);
	}

	protected int getWorkerPort() {
		return workerPort;
	}

	/**
	 * Checks to see if the worker is still running.
	 * 
	 * @return true if the worker is still running
	 */
	protected boolean isRunning() {
		try {
			process.exitValue();
		} catch (IllegalThreadStateException e) {
			return true;
		}
		return false;
	}
	
	
	public String toString() {
		return "  " + this.getClass().getSimpleName() + myID + ":"+this.clientSocket.getLocalPort();
	}
	
	/**
	 * Submits the request to the worker
	 * 
	 * @param acmReq
	 * @return the ValuesMap object returned by the active code
	 */
	protected ValuesMap submitRequest(ActiveCodeMessage acmReq) {
		boolean crashed = false;
		DatagramSocket firstSocket = clientSocket;	

		boolean codeFinished = false;
		String valuesMapString = null;
		
		// Send the request
		ActiveCodeUtils.sendMessage(clientSocket, acmReq, workerPort);
		
		
		//if(ActiveCodeHandler.enableDebugging)
			ActiveCodeHandler.getLogger().log(Level.INFO, this + " send request to the worker's port "+workerPort);
		
		long receivedTime = System.nanoTime();
		// Keeping going until we have received a 'finished' message
		while (!codeFinished) {
			ActiveCodeMessage acmResp = null;
			
			//if(ActiveCodeHandler.enableDebugging)
				ActiveCodeHandler.getLogger().log(Level.INFO, this + " submitRequest waiting for socket message");
			
			acmResp = ActiveCodeUtils.receiveMessage(clientSocket, this.buffer);
			
			//if(ActiveCodeHandler.enableDebugging)
				ActiveCodeHandler.getLogger().log(Level.INFO, this + " submitRequest received socket message: " + (acmResp != null ? "acmResp.valuesMapString = " + acmResp.valuesMapString : "[NULL]"));
			
			/*
			 * If socket timeout is set, then this would work, but result in
			 * some unknown blocking problem
			 */
			if (acmResp == null) {
				crashed = true;
				break;
			}

			if (acmResp.isFinished()) {
				// We are done!
				codeFinished = true;
				valuesMapString = acmResp.getValuesMapString();
				crashed = acmResp.isCrashed();
			} else {
				// We aren't finished, which means that the response asked us to
				// query a guid
				
				/*
				 * Before calling querier to read or write guid, we record the
				 * clientPort, if it is changed after query, it means this task
				 * has timed out and the socket has been reset, so we don't need
				 * to do anything but exit the while loop.
				 */
				String currentGuid = acmReq.getAcp().getGuid();
				ActiveCodeQueryRequest acqreq = acmResp.getAcqreq();
				// Perform the query
				ActiveCodeQueryResponse acqresp = acqh.handleQuery(currentGuid, acqreq);
				if (clientSocket == firstSocket){
					// Send the results back
					ActiveCodeMessage acmres = new ActiveCodeMessage();
					acmres.setAcqresp(acqresp);
					ActiveCodeUtils.sendMessage(clientSocket, acmres, workerPort);
				} else{
					// The port number has been changed, let's get out of the loop
					crashed = true;
					break;
				}
			}
		}
		
		//if(ActiveCodeHandler.enableDebugging)
			ActiveCodeHandler.getLogger().log(Level.INFO, this + " submitRequest out of while(!codeFinished) loop");
		
		
		DelayProfiler.updateDelayNano("activeReceiveMessage", receivedTime);

		long convertTime = System.nanoTime();
		ValuesMap vm = null;

		// Try to convert back to a valuesMap
		if (crashed) {
		    ActiveCodeHandler.getLogger().log(Level.WARNING, "The task running by "+this+" has crashed.");
			try {
				// If there is an error, send the original value back
				vm = new ValuesMap(new JSONObject(acmReq.getAcp().getValuesMapString()));
			} catch (JSONException e) {
				e.printStackTrace();
			}
			return vm;
		} else if (valuesMapString != null) {
			try {
				vm = new ValuesMap(new JSONObject(valuesMapString));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		} else {
			// this should not ever happen
			ActiveCodeHandler.getLogger().log(Level.SEVERE, "ValuesMapString is " + acmReq.toString());
		}
		DelayProfiler.updateDelayNano("activeConvert", convertTime);

		return vm;
	}

	/**
	 * Cleanly shuts down the worker
	 */
	public void shutdownServer() {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setShutdown(true);
		try {
			//submitRequest(acm);
			ActiveCodeUtils.sendMessage(clientSocket, acm, workerPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		clientSocket.close();
	}

	protected boolean isReady() {
		return ready;
	}

	protected synchronized void setReady(boolean ready) {
		ActiveCodeHandler.getLogger().log(Level.INFO, this + " is awaken by notification.");
		this.ready = ready;
		this.notify();
	}

	static class Instrumenter {
		static Set<DatagramSocket> closedSockets = new HashSet<DatagramSocket>();
		static Object monitor = new Object();
		static Set<DatagramSocket> blockedSockets = new HashSet<DatagramSocket>();
	}


	protected void forceShutdownServer() {
		process.destroyForcibly();
		clientSocket.close();
		assert (clientSocket.isClosed());
		//assert (isRunning() == false);

		try {
			clientSocket = new DatagramSocket();
			
			if(ActiveCodeHandler.enableDebugging)
				System.out.println(this + " DESTROYED worker and opened new socket on " + clientSocket.getLocalPort());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected void setNewWorker(int port, Process proc) {
		this.process = proc;
		this.workerPort = port;
	}
	
}
