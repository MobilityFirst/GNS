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
import java.net.DatagramPacket;
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
	JSONParser parser = new JSONParser();
	
	/**
	 * Initialize a RequestHandler in ActiveCodeWorker
	 * @param runner 
	 */
	public RequestHandler(ActiveCodeRunner runner) {
		this.runner = runner;
	}
	
	protected void setPort(int port){
		clientPort = port;
	}
	
	protected boolean handleRequest(DatagramSocket socket) {
		boolean ret = true;
		
		try {			
			// Get the ActiveCodeMessage from the GNS
			ActiveCodeMessage acm = null;

			DatagramPacket pkt = ActiveCodeUtils.receivePacket(socket, buffer);
			acm = (ActiveCodeMessage) (new ObjectInputStream(new ByteArrayInputStream(pkt.getData()))).readObject();
			
			/*
			 * UDP does not guarantee the sequence of the packets, this is the only way for handler to
			 * 
			 */
			if (clientPort == -1){
				setPort(pkt.getPort());
			}
			
		    //FIXME: do not need to initialize new querier everytime
		    ActiveCodeGuidQuerier querier = new ActiveCodeGuidQuerier(socket, clientPort);
		    
		    if( acm.isShutdown() ) {
		    	//System.out.println("Shutting down...");
		    	ret = false;
		    } else {
		    	// Run the active code
		    	long t1 = System.currentTimeMillis();
			    ActiveCodeParams params = acm.getAcp();			    
			   
			    assert(params != null);
			    
			    querier.setParam(params.getHopLimit(), params.getGuid());
			    
			    JSONObject vm = (JSONObject) parser.parse(params.getValuesMapString());
			    
			    DelayProfiler.updateDelayNano("activeWorkerPrepare", t1);
			    
			    JSONObject result = runner.runCode(params.getGuid(), params.getAction(), params.getField(), params.getCode(), vm, querier);
			    
			    //System.out.println(">>>>>>>>>>>>>>>>It takes "+(System.currentTimeMillis()-t1)+"ms to execute this normal code!");
			    
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
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally{
			
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
