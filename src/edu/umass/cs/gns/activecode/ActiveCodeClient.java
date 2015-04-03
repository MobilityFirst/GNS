package edu.umass.cs.gns.activecode;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gns.nsdesign.GnsApplicationInterface;
import edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable;
import edu.umass.cs.gns.util.ResultValue;
import edu.umass.cs.gns.util.ValuesMap;

public class ActiveCodeClient {
	private String hostname;
	private int port;
	private boolean restartOnCrash;
	private Process process;
	private GnsApplicationInterface app;
	
	public ActiveCodeClient(GnsApplicationInterface app, boolean launchServer, boolean restartOnCrash) {
		if(launchServer)
			startServer();
		
		this.restartOnCrash = restartOnCrash;
		this.app = app;
	}
	
	public ActiveCodeClient(GnsReconfigurable app, String hostname, int port,  boolean restartOnCrash) {
		this.restartOnCrash = restartOnCrash;
		this.hostname = hostname;
		this.port = port;
		this.app = app;
		
	}
	
	/**
	 * Grab an open port
	 * @return
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
	
	public boolean startServer() {
		try {
			List<String> command = new ArrayList<String>();
			port = getOpenPort();
			hostname = "0.0.0.0";
			// Get the current classpath
			String classpath = System.getProperty("java.class.path");
			
		    command.add("java");
		    command.add("-Xms64m");
		    command.add("-Xmx64m");
		    command.add("-cp");
		    command.add(classpath);
		    command.add("edu.umass.cs.gns.activecode.worker.ActiveCodeWorker");
		    command.add(Integer.toString(port));
		    
		    ProcessBuilder builder = new ProcessBuilder(command);
			builder.directory(new File(System.getProperty("user.dir")));
			builder.redirectError(Redirect.INHERIT);
			builder.redirectOutput(Redirect.INHERIT);
			builder.redirectInput(Redirect.INHERIT);

			process = builder.start();
		    
		    Thread.sleep(1000);
		    
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public ValuesMap runActiveCode(ActiveCodeParams acp) {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.acp = acp;
		// Send the request to the worker
		ValuesMap vm = submitRequest(acm);
		
		// Restart the server if it crashed
		if(restartOnCrash && !isRunning())
			startServer();
		
		return vm;
	}
	
	/**
	 * Checks to see if the worker is still running
	 * @return
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
	 * @return
	 */
	private ValuesMap submitRequest(ActiveCodeMessage acmReq) {
		try {
			Socket socket = new Socket(hostname, port);
			PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			
			ActiveCodeQueryHelper acqh = new ActiveCodeQueryHelper(app);
			
			// Serialize the initial request
			ActiveCodeUtils.sendMessage(out, acmReq);
			
			boolean codeFinished = false;
			String valuesMapString = null;
			
			// Keeping going until we have received a 'finished' message
			while(!codeFinished) {
			    ActiveCodeMessage acmResp = ActiveCodeUtils.getMessage(in);
			    
			    if(acmResp.finished) {
			    	// We are done!
			    	codeFinished = true;
			    	valuesMapString = acmResp.valuesMapString;
			    }
			    else {
			    	// We aren't finished, which means that the response asked us to query a guid
			    	// Can be read or write query (writes are only supported locally)
			    	String currentGuid = acmReq.acp.guid;
			    	ActiveCodeQueryRequest acqreq = acmResp.acqreq;
			    	// Perform the query
			    	ActiveCodeQueryResponse acqresp = acqh.handleQuery(currentGuid, acqreq);
			    	// Send the results back
			    	ActiveCodeMessage acmres = new ActiveCodeMessage();
			    	acmres.acqresp = acqresp;
			    	ActiveCodeUtils.sendMessage(out, acmres);
			    }
			}
			
	        socket.close();
	        
	        ValuesMap vm = null;
	        
	        // Try to convert back to a valuesMap
	        if(valuesMapString != null) {
	        	try {
	        		vm = new ValuesMap(new JSONObject(valuesMapString));
	 	        } catch (JSONException e) {
	 	        	e.printStackTrace();
	 	        }
	        }
	        
	        return vm;
	        
		} catch (IOException e) {
			e.printStackTrace();
		}
      
		return null;
	}

	/**
	 * Cleanly shuts down the worker
	 */
	public void shutdownServer() {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.shutdown = true;
		submitRequest(acm);
	}
}
