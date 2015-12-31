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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

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
	private String hostname;
	private int port;
	private Process process;
	private final GnsApplicationInterface<?> app;
	
	/**
	 * @param app the gns app
         * @param launchWorker
	 */
	public ActiveCodeClient(GnsApplicationInterface<?> app, boolean launchWorker) {
		if(launchWorker)
			startServer();
		
		this.app = app;
	}
	
	private class StreamGobbler extends Thread {
	    InputStream is;
	    String type;

	    private StreamGobbler(InputStream is, String type) {
	        this.is = is;
	        this.type = type;
	    }

	    @Override
	    public void run() {
	        try {
	            InputStreamReader isr = new InputStreamReader(is);
	            BufferedReader br = new BufferedReader(isr);
	            String line = null;
	            while ((line = br.readLine()) != null)
	                System.out.println(type + "> " + line);
	        }
	        catch (IOException ioe) {
	            ioe.printStackTrace();
	        }
	    }
	}
	
	/**
	 * Grab an open port
	 * @return the port number
	 */
	public static int getOpenPort() {
		int port = 0;
		try {
			ServerSocket listener = new ServerSocket(0);
			port = listener.getLocalPort();
			listener.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return port;
	}
	
	/**
	 * Starts an active code worker and waits for it to accept requests
	 * @return true if successful
	 */
	public boolean startServer() {
		try {
			List<String> command = new ArrayList<>();
			port = getOpenPort();
			hostname = "0.0.0.0";
			// Get the current classpath
			String classpath = System.getProperty("java.class.path");
			
			ServerSocket listener = new ServerSocket(0);
			
		    command.add("java");
		    command.add("-Xms64m");
		    command.add("-Xmx64m");
		    command.add("-cp");
		    command.add(classpath);
		    command.add("edu.umass.cs.gnsserver.activecode.worker.ActiveCodeWorker");
		    command.add(Integer.toString(port));
		    command.add(Integer.toString(listener.getLocalPort()));
		    
		    ProcessBuilder builder = new ProcessBuilder(command);
			builder.directory(new File(System.getProperty("user.dir")));
			
			//builder.redirectError(Redirect.INHERIT);
			//builder.redirectOutput(Redirect.INHERIT);
			//builder.redirectInput(Redirect.INHERIT);
			
			process = builder.start();
			StreamGobbler outputGobbler = new StreamGobbler(process.getInputStream(), "OUTPUT");
			outputGobbler.start();
			// Now we wait for the worker to notify us that it is ready
			listener.accept();
			listener.close();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}  
	
	/**
	 * 
	 * @param acp the parameters to send to the worker
	 * @param useTimeout whether or not to use the timeout when waiting for a reply
	 * @return the ValuesMap object returned by the active code
	 */
	public ValuesMap runActiveCode(ActiveCodeParams acp, boolean useTimeout) throws ActiveCodeException{
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.setAcp(acp);
		ValuesMap vm = null;
		
		try{
			vm = submitRequest(acm);
		}catch(InterruptedException e){
			e.printStackTrace();
		}catch(ActiveCodeException e){			
			//e.printStackTrace();
			throw new ActiveCodeException();
		}
		
		return vm;
	}
	
	/**
	 * Checks to see if the worker is still running.
	 * @return true if the worker is still running
	 */
	private boolean isRunning() {
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
	 * @throws IOException 
	 * @throws ActiveCodeException 
	 */
	private ValuesMap submitRequest(ActiveCodeMessage acmReq) throws InterruptedException, ActiveCodeException{
		long startTime = System.nanoTime();
		Socket socket = null;
		boolean crashed = false;
		
		PrintWriter out = null;
		BufferedReader in = null;
		
		ActiveCodeQueryHelper acqh = new ActiveCodeQueryHelper(app);
		
		boolean codeFinished = false;
		String valuesMapString = null;
		
		// Create the socket and throw an exception upon failure
		try{
			socket = new Socket(hostname, port);
			//socket.setSoTimeout(timeoutMs);
			
			out = new PrintWriter(socket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
						
		}catch(IOException e){
			e.printStackTrace();
		}
		
		// Serialize the initial request
		ActiveCodeUtils.sendMessage(out, acmReq);
		DelayProfiler.updateDelayNano("activeSendMessage", startTime);
		
		long receivedTime = System.nanoTime();
		// Keeping going until we have received a 'finished' message
		while(!codeFinished) {
		    ActiveCodeMessage acmResp = ActiveCodeUtils.getMessage(in);
		    
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
		    	ActiveCodeUtils.sendMessage(out, acmres);
		    }
		}
	
		// Done with socket at this point
        try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		DelayProfiler.updateDelayNano("activeReceiveMessage", receivedTime);
		
		long convertTime = System.nanoTime();
		ValuesMap vm = null;
	        
        // Try to convert back to a valuesMap
        if(crashed) {
        	System.out.println("################### "+ acmReq.getAcp().getGuid()+" Crashed! ####################");
        	throw new ActiveCodeException();
        }else if(valuesMapString != null) {
        	try {
        		vm = new ValuesMap(new JSONObject(valuesMapString));
 	        } catch (JSONException e) {
 	        	e.printStackTrace();
 	        }
        }else{
        	System.out.println("The returned value is "+valuesMapString);
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
			submitRequest(acm);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Restart the worker stuck with a malicious code
	 */
	public void restartServer() {
		long t1 = System.currentTimeMillis();
		process.destroyForcibly();		
		startServer();
		long elapsed = System.currentTimeMillis() - t1;
		System.out.println("It takes "+elapsed+"ms to restart this worker.");
		
	}
}
