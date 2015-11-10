/*
 * Copyright (C) 2015
 * University of Massachusetts
 * All Rights Reserved 
 *
 */
package edu.umass.cs.gnsserver.activecode.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.ActiveCodeUtils;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gnsserver.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gnsserver.utils.ValuesMap;


public class RequestHandler {
	private ActiveCodeRunner runner;

	public RequestHandler(ActiveCodeRunner runner) {
		this.runner = runner;
	}
	
	public boolean handleRequest(Socket socket) {
		boolean ret = true;
		
		try {
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			ActiveCodeGuidQuerier querier = new ActiveCodeGuidQuerier(in, out);
			// Get the ActiveCodeMessage from the GNS
		    ActiveCodeMessage acm = ActiveCodeUtils.getMessage(in);
		    
		    if( acm.isShutdown()) {
		    	out.println("OK");
		    	System.out.println("Shutting down...");
		    	ret = false;
		    } else {
		    	// Run the active code
			    ActiveCodeParams params = acm.getAcp();
			    ValuesMap vm = new ValuesMap(new JSONObject(params.getValuesMapString()));
			    ValuesMap result = runner.runCode(params.getGuid(), params.getAction(), params.getField(), params.getCode(), vm, querier);
			    // Send the results back
			    ActiveCodeMessage acmResp = new ActiveCodeMessage();
			    acmResp.setFinished(true);
			    acmResp.setValuesMapString(result == null ? null : result.toString());
			    ActiveCodeUtils.sendMessage(out, acmResp);
		    }
		    
		} catch (IOException | JSONException e) {
			e.printStackTrace();
			ret = false;
		}
		
		return ret;
	}
}
