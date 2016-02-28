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

import java.net.DatagramSocket;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class accepts the request from active client
 * and run active code in the worker.
 * 
 * @author Zhaoyu Gao
 */
public class RequestHandler {
	private ActiveCodeRunner runner;
	private int clientPort = -1;
	private byte[] buffer = new byte[8096*10];
	
	/**
	 * Initialize a RequestHandler in ActiveCodeWorker
	 * @param runner
	 * @param port 
	 */
	public RequestHandler(ActiveCodeRunner runner, int port) {
		this.runner = runner;
		this.clientPort = port;
		
	}
	
	protected void setPort(int port){
		clientPort = port;
	}
	
	protected boolean handleRequest(DatagramSocket socket) {
		boolean ret = true;
		
		try {			
			// Get the ActiveCodeMessage from the GNS
			ActiveCodeMessage acm = null;

			acm = ActiveCodeUtils.receiveMessage(socket, buffer);
			
			
		    //FIXME: do not need to initialize new querier everytime
		    ActiveCodeGuidQuerier querier = new ActiveCodeGuidQuerier(socket, clientPort);
		    
		    if( acm.isShutdown() ) {
		    	System.out.println("Shutting down...");
		    	ret = false;
		    } else if(acm.getAcqresp() != null) {
		    	// notify and return
		    	querier.setResponse(acm.getAcqresp());
		    	return true;
		    }
		    else {
		    	// Run the active code
		    	long t1 = System.nanoTime();
			    ActiveCodeParams params = acm.getAcp();			    
			   
			    //System.out.println("Got the message from port "+socket.getLocalPort());
			    assert(params != null);
			    
			    //System.out.println("The hop is "+params.getHopLimit()+". The guid is "+params.getGuid());
			    querier.setParam(params.getHopLimit(), params.getGuid());
			    JSONParser parser = new JSONParser();
			    JSONObject vm = (JSONObject) parser.parse(params.getValuesMapString());
			    
			    DelayProfiler.updateDelayNano("activeWorkerPrepare", t1);
			    
			    JSONObject result = runner.runCode(params.getGuid(), params.getAction(), params.getField(), params.getCode(), vm, querier);
			    
			    long t2 = System.nanoTime();
			    
			    // Send the results back
			    ActiveCodeMessage acmResp = new ActiveCodeMessage();
			    acmResp.setFinished(true);
			    acmResp.setCrashed(querier.getError());
			    acmResp.setValuesMapString(result == null ? null : result.toString());
			    ActiveCodeUtils.sendMessage(socket, acmResp, clientPort);
			    //System.out.println("Send the response back through port "+socket.getLocalPort());
			    DelayProfiler.updateDelayNano("activeWorkerAfterRun", t2);
		    }
		    
		} catch (ParseException e ) {
			ActiveCodeMessage acmResp = crashedMessage(e.toString());
			ActiveCodeUtils.sendMessage(socket, acmResp, clientPort);
			return ret;
		}
		
		return ret;
	}
	
	
	private ActiveCodeMessage crashedMessage(String errMsg){
		ActiveCodeMessage acmResp = new ActiveCodeMessage();
		acmResp.setFinished(true);
	    acmResp.setValuesMapString(null);
	    acmResp.setCrashed(errMsg);
	    return acmResp;
	}
}
