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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.DatagramSocket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.utils.ValuesMap;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class accepts the request from active client
 * and run active code in the worker.
 * 
 * @author Zhaoyu Gao
 */
public class RequestHandler {
	private ActiveCodeRunner runner;
	private int clientPort = 0;
	private byte[] buffer = new byte[8096*10];
	/**
	 * Initialize a RequestHandler in ActiveCodeWorker
	 * @param runner
	 */
	public RequestHandler(ActiveCodeRunner runner, int port) {
		this.runner = runner;
		this.clientPort = port;
	}
	
	protected boolean handleRequest(DatagramSocket socket) {
		boolean ret = true;
		
		try {
			long startTime = System.nanoTime();
			//PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			//BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			//ActiveCodeGuidQuerier querier = new ActiveCodeGuidQuerier(in, out);
			// Get the ActiveCodeMessage from the GNS
		    ActiveCodeMessage acm = ActiveCodeUtils.receiveMessage(socket, this.buffer);
		    
		    
		    if( acm.isShutdown()) {
		    	//FIXME: the following line is used to send back a response
		    	//out.println("OK");
		    	System.out.println("Shutting down...");
		    	ret = false;
		    } else {
		    	// Run the active code
		    	long t1 = System.nanoTime();
			    ActiveCodeParams params = acm.getAcp();
			    //querier.setParam(params.getHopLimit(), params.getGuid());
			    //FIXME: This step takes too much time
			    //ValuesMap vm = new ValuesMap(new JSONObject(params.getValuesMapString()));
			    JSONObject vm = new JSONObject(params.getValuesMapString());
			    
			    DelayProfiler.updateDelayNano("activeWorkerPrepare", t1);
			    
			    JSONObject result = runner.runCode(params.getGuid(), params.getAction(), params.getField(), params.getCode(), vm, null);
			    
			    long t2 = System.nanoTime();
			    // Send the results back
			    ActiveCodeMessage acmResp = new ActiveCodeMessage();
			    acmResp.setFinished(true);
			    acmResp.setValuesMapString(result == null ? null : result.toString());
			    ActiveCodeUtils.sendMessage(socket, acmResp, this.clientPort);
			    DelayProfiler.updateDelayNano("activeWorkerAfterRun", t2);
		    }
		    
		} catch (JSONException e) {
			e.printStackTrace();
			ret = false;
		}
		
		return ret;
	}
}
