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
package edu.umass.cs.gnsserver.activecode.worker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.management.ManagementFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import edu.umass.cs.gnsserver.activecode.ActiveCodeGuardian;
import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class is the worker to run active code
 * in an isolated process.
 *
 * @author Zhaoyu Gao
 */
public class ActiveCodeWorker {
	/**
	 * the runner and querier is used for executor to execute the request
	 */
	private ActiveCodeRunner runner;
	private ActiveCodeGuidQuerier querier;
	
	private DatagramSocket serverSocket;
	private byte[] buffer = new byte[2048];
	private int clientPort = -1;
	
	/**
	 * executor is used for executing requests from the client, it can only execute one request at a time
	 */
	private ExecutorService executor = Executors.newSingleThreadExecutor();
	
	
	private java.lang.management.OperatingSystemMXBean mxbean =
			  (java.lang.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
	
	private long last = 0;
	private long elapsed = 0;
	private final static long timeout = -1;// AppReconfigurableNodeOptions.activeCodeTimeOut;
	
	/**
	 * variables for instrument only
	 */
	protected static int numReqs = 0;
	
	/**
	 * @param port
	 */
	public ActiveCodeWorker(int port) {
		try{
			this.serverSocket = new DatagramSocket(port);
			System.out.println(this+" : Starting ActiveCodeWorker at port number " + port);
		}catch(IOException e){
			e.printStackTrace();
		}
	}
	
	public String toString(){
		return "ActiveCodeWorker"+serverSocket.getLocalPort();
	}
	
	/**
	 * Starts the worker listener 
	 * @throws IOException
	 */
	public void run() throws IOException {
		// Initialize the runner with the script engine
        runner = new ActiveCodeRunner(serverSocket);
        querier = new ActiveCodeGuidQuerier();
        
    	//RequestHandler handler = new RequestHandler(runner, serverSocket);
    	boolean keepGoing = true;

		// Notify the server that I'm ready    	
		ActiveCodeUtils.sendMessage(serverSocket, new ActiveCodeMessage(), 60000);
		
        while (keepGoing) {
        	keepGoing = handleRequest();       	
        	numReqs++;
        	if(numReqs%1000 == 0){
        		System.out.println(DelayProfiler.getStats());
        	}
        	
        	elapsed = 0;
        }        
        serverSocket.close();
	}
	
	private boolean handleRequest(){
		boolean ret = true;
		Throwable thr = null;
		try {			
			// Get the ActiveCodeMessage from the GNS
			ActiveCodeMessage acm = null;

			DatagramPacket pkt = ActiveCodeUtils.receivePacket(serverSocket, buffer);
			acm = (ActiveCodeMessage) (new ObjectInputStream(new ByteArrayInputStream(pkt.getData()))).readObject();
			
			int incommingPort = pkt.getPort();
			/*
			 * UDP does not guarantee the sequence of the packets, this is the only way for handler to
			 */
			if (clientPort != pkt.getPort() && incommingPort != ActiveCodeGuardian.guardPort){
				clientPort = incommingPort;
				// set the client port in runner and querier
				runner.setClientPort(incommingPort);
				querier.setClientPort(incommingPort);
			}
						
			if( acm.isShutdown() ) {
		    	ret = false;
		    } else if (acm.isCrashed() ){
		    	/**
		    	 * send back acknowledgement to notify the guard that this worker has dealt with
		    	 * the timedout task by itself. 
		    	 */
		    	
		    	
		    	assert(acm.error != null && acm.error.equals(ActiveCodeUtils.TIMEOUT_ERROR));
		    	
		    	acm.setCrashed(null);
		    	
		    	if (elapsed > timeout ){		    		
		    		// querier's socket needs to be shutdown no matter what
		    		querier.shutdownAndRestartSocket();
		    		
		    		// time out shutdown the executor and restart a new one
		    		executor.shutdownNow();
		    		executor = Executors.newSingleThreadExecutor();
		    		acm.setCrashed(ActiveCodeUtils.TIMEOUT_ERROR);
		    		
		    	}
		    	ActiveCodeUtils.sendMessage(serverSocket, acm, ActiveCodeGuardian.guardPort);		    	
		    	
		    } else {
		    	// Run the active code
			    ActiveCodeParams params = acm.getAcp();
			    
			    /*
			     * Invariant: the params should not be null
			     */
			    assert(params != null);
			    
			    querier.setParam(params.getHopLimit(), params.getGuid());
			    executor.execute(new ActiveCodeWorkerTask(params, runner, querier));
		    }
			
		}catch (ClassNotFoundException e) {
			thr = e;
		} catch (IOException e) {
			thr = e;
		} finally{		
			if(thr != null){
				thr.printStackTrace();
			}
		}				
		return ret;
	}
	
	/**
	 * Test single worker
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException  {
		int port = 0;
		port = Integer.parseInt(args[0]);
		
		ActiveCodeWorker acs = new ActiveCodeWorker(port);
		acs.run();
    
  }

}
