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
	private final GnsApplicationInterface<String> app;
	private DatagramSocket clientSocket;
	private byte[] buffer = new byte[8096];
	protected final int myID = getNextID();
	
	
	private static int globalID = getNextID();
	private synchronized static int getNextID() {
		return globalID++;
	}

	/**
	 * @param app
	 *            the gnsApp
	 * @param port
	 * @param proc
	 */
	public ActiveCodeClient(GnsApplicationInterface<String> app, int port, Process proc) {
		this.app = app;
		this.ready = false;
		// initialize the clientSocket first
		try {
			clientSocket = new DatagramSocket();
			
		} catch (IOException e) {
			e.printStackTrace();
		}

		setNewWorker(port, proc);
	}

	protected int getPort() {
		return workerPort;
	}

	/**
	 * 
	 * @param acp
	 *            the parameters to send to the worker
	 * @return the ValuesMap object returned by the active code
	 */
	public ValuesMap runActiveCode(ActiveCodeParams acp) {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setAcp(acp);
		ValuesMap vm = null;

		vm = submitRequest(acm);

		return vm;
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
	 * Submits the request to the worker via socket comm.
	 * 
	 * @param acmReq
	 * @return the ValuesMap object returned by the active code
	 */
	protected ValuesMap submitRequest(ActiveCodeMessage acmReq) {
		long startTime = System.nanoTime();
		boolean crashed = false;

		ActiveCodeQueryHelper acqh = new ActiveCodeQueryHelper(app);

		boolean codeFinished = false;
		String valuesMapString = null;
		/*
		 * Invariant: this thread can't be interrupted, because
		 */
		assert (!Thread.currentThread().isInterrupted());

		// Send the request
		ActiveCodeUtils.sendMessage(this.clientSocket, acmReq, workerPort);
		DelayProfiler.updateDelayNano("activeSendMessage", startTime);

		long receivedTime = System.nanoTime();
		// Keeping going until we have received a 'finished' message
		while (!codeFinished) {
			ActiveCodeMessage acmResp = null;
			if(ActiveCodeHandler.enableDebugging)
				System.out.println(this + " submitRequest waiting for socket message");
			
			acmResp = ActiveCodeUtils.receiveMessage(clientSocket, this.buffer);
			
			if(ActiveCodeHandler.enableDebugging)
				System.out.println(this + " submitRequest received socket message: " + (acmResp != null ? "acmResp.valuesMapString = " + acmResp.valuesMapString : "[NULL]"));
			
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
				 * Before calling querier to read or write guid, we need to record the
				 * clientPort number, if it is changed after query, it means this task
				 * has timed out and the socket has been reset, so we don't need to do
				 * anything but exit the while loop.
				 */
				int previousPort = clientSocket.getLocalPort();
				String currentGuid = acmReq.getAcp().getGuid();
				ActiveCodeQueryRequest acqreq = acmResp.getAcqreq();
				// Perform the query
				ActiveCodeQueryResponse acqresp = acqh.handleQuery(currentGuid, acqreq);
				if (clientSocket.getLocalPort() == previousPort){
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
		if(ActiveCodeHandler.enableDebugging)
			System.out.println(this + " submitRequest out of while(!codeFinished) loop");

		DelayProfiler.updateDelayNano("activeReceiveMessage", receivedTime);

		long convertTime = System.nanoTime();
		ValuesMap vm = null;

		// Try to convert back to a valuesMap
		if (crashed) {
			//System.out.println("################### " + acmReq.getAcp().getGuid() + " Crashed! #################### "
			//		+ acmReq.getValuesMapString());
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
			System.out.println("ValuesMapString is " + acmReq.toString());
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
			//submitRequest(acm);
			while(!isReady()){
				this.wait();
			}
			ActiveCodeUtils.sendMessage(clientSocket, acm, workerPort);
		} catch (Exception e) {
			e.printStackTrace();
		}
		clientSocket.close();
	}

	protected boolean isReady() {
		return ready;
	}

	protected void setReady(boolean ready) {
		this.ready = ready;
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
		
		notifyWorkerOfClientPort(port);
	}

	protected void notifyWorkerOfClientPort(int port) {
		// notify new worker about the client's new port number
		ActiveCodeUtils.sendMessage(clientSocket, new ActiveCodeMessage(), port);
	}
}
