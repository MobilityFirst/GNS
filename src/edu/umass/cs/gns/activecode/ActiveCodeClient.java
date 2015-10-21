package edu.umass.cs.gns.activecode;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gns.activecode.protocol.ActiveCodeMessage;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeParams;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeQueryRequest;
import edu.umass.cs.gns.activecode.protocol.ActiveCodeQueryResponse;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.gnsApp.GnsApplicationInterface;
import edu.umass.cs.gns.utils.ValuesMap;

public class ActiveCodeClient {
	private String hostname;
	private int port;
	private boolean restartOnCrash;
	private Process process;
	private GnsApplicationInterface app;
	private boolean killed;
	
	/**
	 * @param app the gns app
	 * @param launchServer whether or not to launch the active code worker
	 */
	public ActiveCodeClient(GnsApplicationInterface app, boolean launchWorker) {
		if(launchWorker)
			startServer();
		
		this.app = app;
	}
	
	/**
//	 * @param app the gns app
//	 * @param hostname the hostname of the running worker
//	 * @param port the port of the running worker
//	 */
//	public ActiveCodeClient(GnsReconfigurable app, String hostname, int port) {
//		this.hostname = hostname;
//		this.port = port;
//		this.app = app;
//	}
	
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
	
	/**
	 * Starts an active code worker and waits for it to accept requests
	 * @return
	 */
	public boolean startServer() {
		try {
			List<String> command = new ArrayList<String>();
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
		    command.add("edu.umass.cs.gns.activecode.worker.ActiveCodeWorker");
		    command.add(Integer.toString(port));
		    command.add(Integer.toString(listener.getLocalPort()));
		    
		    ProcessBuilder builder = new ProcessBuilder(command);
			builder.directory(new File(System.getProperty("user.dir")));
			builder.redirectError(Redirect.INHERIT);
			builder.redirectOutput(Redirect.INHERIT);
			builder.redirectInput(Redirect.INHERIT);

			process = builder.start();
			
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
	 * @returnthe ValuesMap object returned by the active code
	 * @throws ActiveCodeException
	 */
	public ValuesMap runActiveCode(ActiveCodeParams acp, boolean useTimeout) throws ActiveCodeException {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.acp = acp;
		ValuesMap vm = null;
		
		// Send the request to the worker
		try {
			int timeout = useTimeout ? 500 : 0;
			vm = submitRequest(acm, timeout);
		}
		catch(ActiveCodeException e) {
			GNS.getLogger().warning("Code failed to return in allotted time!");
			process.destroyForcibly();
			throw new ActiveCodeException();
		}
		
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
	 * @return the ValuesMap object returned by the active code
	 * @throws IOException 
	 * @throws ActiveCodeException 
	 */
	private ValuesMap submitRequest(ActiveCodeMessage acmReq, int timeoutMs) throws ActiveCodeException {
		Socket socket = null;
		boolean crashed = false;
		
		PrintWriter out;
		BufferedReader in;
		
		// Create the socket and throw an exception upon failure
		try {
			socket = new Socket(hostname, port);
			socket.setSoTimeout(timeoutMs);
			
			out = new PrintWriter(socket.getOutputStream(), true);
			in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		} catch (IOException e) {
			throw new ActiveCodeException();
		}
		
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
		    	crashed = acmResp.crashed;
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
		
		// Done with socket at this point
        try {
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
        
        ValuesMap vm = null;
        
        // Try to convert back to a valuesMap
        if(crashed) {
        	throw new ActiveCodeException();
        }
        else if(valuesMapString != null) {
        	try {
        		vm = new ValuesMap(new JSONObject(valuesMapString));
 	        } catch (JSONException e) {
 	        	e.printStackTrace();
 	        }
        }
        
        return vm;
	}

	/**
	 * Cleanly shuts down the worker
	 */
	public void shutdownServer() {
		ActiveCodeMessage acm = new ActiveCodeMessage();
		acm.shutdown = true;
		try {
			submitRequest(acm, 0);
		} catch (ActiveCodeException e) {
			e.printStackTrace();
		}
	}
}
