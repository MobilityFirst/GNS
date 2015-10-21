package edu.umass.cs.gns.activecode.worker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.activecode.ActiveCodeUtils;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.utils.ValuesMap;


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
		    
		    if(acm.shutdown) {
		    	out.println("OK");
		    	System.out.println("Shutting down...");
		    	ret = false;
		    } else {
		    	// Run the active code
			    ActiveCodeParams params = acm.acp;
			    ValuesMap vm = new ValuesMap(new JSONObject(params.valuesMapString));
			    ValuesMap result = runner.runCode(params.guid, params.action, params.field, params.code, vm, querier);
			    // Send the results back
			    ActiveCodeMessage acmResp = new ActiveCodeMessage();
			    acmResp.finished = true;
			    acmResp.valuesMapString = result == null ? null : result.toString();
			    ActiveCodeUtils.sendMessage(out, acmResp);
		    }
		    
		} catch (IOException | JSONException e) {
			e.printStackTrace();
			ret = false;
		}
		
		return ret;
	}
}
