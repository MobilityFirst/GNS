package edu.umass.cs.gns.nio;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;


/**
@author V. Arun
 */

/* This class exists primarily as a GNS wrapper around NIOTransport. NIOTransport 
 * is for general-purpose NIO byte stream communication between numbered nodes as 
 * specified by the NodeConfig interface and a data processing worker as specified 
 * by the DataProcessingWorker interface. 
 * 
 * The GNS-specific functions include (1) delay emulation, (2) short-circuiting 
 * local sends by directly sending it to packetDemux, and (3) adding GNS-specific 
 * headers to NIO's byte stream abstraction, and (4) supporting a crazy number of 
 * redundant public methods to do different kinds of sends. These methods exist 
 * only for backwards compatibility.  
 * 
 * 
 */
public class GNSNIOTransport extends NIOTransport {
	
	Timer timer = new Timer();
	
	public GNSNIOTransport(int id, NodeConfig nodeConfig, JSONMessageWorker worker) throws IOException {
		super(id, nodeConfig, worker); // Switched order of the latter two arguments
	}
	
	/********************Start of send methods*****************************************/
	/* A sequence of ugly public methods that are essentially redundant ways of 
	 * invoking send() on NIOTransport. They exist primarily for backwards compatibility
	 * and must be cleaned up to a smaller number that is really needed. None of these 
	 * methods actually sends on a socket. That is done by a single private method that
	 * invokes the underlying send in the parent.
	 * 
	 * These methods are undocumented because it is not clear which classes are designed
	 * to call which of these methods. They have been copied over from the older NioServer
	 * and need to be documented or overhauled completely.
	 */
	public void sendToIDs(Set<Integer> destIDs, JSONObject jsonData) throws IOException {
		sendToIDs(destIDs, jsonData, -1);
	}

	public void sendToIDs(short[] destIDs, JSONObject jsonData) throws IOException {
		sendToIDs(destIDs, jsonData, -1);
	}

	public void sendToIDs(short[]destIDs, JSONObject jsonData, int excludeID) throws IOException {
		TreeSet<Integer> IDs = new TreeSet<Integer>();
		for (int destID: destIDs) {
			IDs.add((int)destIDs[destID]);
		}
		sendToIDs(IDs, jsonData, excludeID);
	}

	public void sendToIDs(Set<Integer> destIDs, JSONObject jsonData, int excludeID) throws IOException {
		for (int destID:destIDs) {
			if (destID == this.myID || destID == excludeID) continue;
			sendToID(destID, jsonData);
		}
	}

	public boolean sendToID(int id, JSONObject jsonData) throws IOException {
		GNSDelayEmulator.sendWithDelay(timer, this, id, jsonData);
		return true;
	}

	/* This method returns true for no good reason. Nobody checks the return
	 * value. Exists only for backwards compatibility. It must be corrected to void. 
	 * 
	 * This method adds a header only if a socket channel is used to send to
	 * a remote node, otherwise it hands over the message directly to the worker.
	 */
	public boolean sendToIDActual(int destID, JSONObject jsonData) throws IOException {
		if(destID==this.myID) {
			ArrayList<JSONObject> jsonArray = new ArrayList<JSONObject>();
			jsonArray.add(jsonData);
			((JSONMessageWorker)worker).processJSONMessages(jsonArray);
		}
		else {
			String headeredMsg = JSONMessageWorker.prependHeader(jsonData.toString());
			this.sendUnderlying(destID, headeredMsg.getBytes());
		}
		return true;
	}
	/********************End of public send methods*****************************************/	
	
	/* This method is really redundant. But it exists so that there is one place where
	 * all NIO sends actually happen given the maddening number of different public send
	 * methods above. Do NOT add more gunk to this method.
	 */
	private void sendUnderlying(int id, byte[] data) throws IOException {
		this.send(id, data);
	}
	private static JSONObject JSONify(String s) throws JSONException{
		return new JSONObject("{\"msg\" : \"" + s + "\"}");
	}
	
	/* The test code here is mostly identical to that of NIOTransport but tests
	 * JSON messages, headers, and delay emulation features. Need to test it with 
	 * the rest of GNS.
	 */
	public static void main(String[] args) {
		int port = 2000;
		int nNodes=100;
		SampleNodeConfig snc = new SampleNodeConfig(port);
		snc.localSetup(nNodes);
		JSONMessageWorker worker = new JSONMessageWorker(new DefaultPacketDemultiplexer());
		GNSNIOTransport[] niots = new GNSNIOTransport[nNodes];
		
		try {
			int smallNNodes = 2;
			for(int i=0; i<smallNNodes; i++) {
				niots[i] = new GNSNIOTransport(i, snc, worker);
				new Thread(niots[i]).start();
			}			
			
			/*************************************************************************/
			/* Test a few simple hellos. The sleep is there to test 
			 * that the successive writes do not "accidentally" benefit
			 * from concurrency, i.e., to check that OP_WRITE flags will
			 * be set correctly.
			 */
			niots[1].sendToIDActual(0, JSONify("Hello from 1 to 0"));
			niots[0].sendToIDActual(1, JSONify("Hello back from 0 to 1"));
			niots[0].sendToIDActual(1, JSONify("Second hello back from 0 to 1"));
			try {Thread.sleep(1000);} catch(Exception e){e.printStackTrace();}
			niots[0].sendToIDActual(1, JSONify("Third hello back from 0 to 1"));
			niots[1].sendToIDActual(0, JSONify("Thank you for all the hellos back from 1 to 0"));
			/*************************************************************************/
			
			Thread.sleep(2000);
			System.out.println("\n\n\nBeginning test of random, sequential communication pattern");
			Thread.sleep(1000);
			
			/*************************************************************************/
			//Create the remaining nodes up to nNodes
			for(int i=smallNNodes; i<nNodes; i++) {
				niots[i] = new GNSNIOTransport(i, snc, worker);
				new Thread(niots[i]).start();
			}			
			
			// Test a random, sequential communication pattern
			for(int i=0; i<nNodes; i++) {
				int k = (int)(Math.random()*nNodes);
				int j = (int)(Math.random()*nNodes);
				System.out.println("Message " + i);
				niots[k].sendToIDActual(j, JSONify("Hello from " + k + " to " + j));
			}
			/*************************************************************************/
			Thread.sleep(1000);
			System.out.println("\n\n\nBeginning test of random, concurrent communication pattern ***with emulated delays***");
			Thread.sleep(1000);
			/*************************************************************************/
			// Test a random, concurrent communication pattern with emulated delays
			Timer T = new Timer();
			class TX extends TimerTask {
				private int sndr=-1;
				private int rcvr=-1;
				private GNSNIOTransport[] niots=null;
				TX(int i, int j, GNSNIOTransport[] n) {
					sndr = i;
					rcvr = j;
					niots = n;
				}
				public void run() {
					try {
						niots[sndr].sendToID(rcvr, JSONify("Hello from " + sndr + " to " + rcvr));
					} catch(IOException e) {
						e.printStackTrace();
					} catch(JSONException e) {
						e.printStackTrace();
					}
				}
			}
			GNSDelayEmulator.emulateDelays();
			for(int i=0; i<nNodes; i++) {
				int k = (int)(Math.random()*nNodes);
				int j = (int)(Math.random()*nNodes);
				long millis = (long)(Math.random()*1000);
				TX task = new TX(k, j, niots);
				System.out.println("Scheduling message " + i);
				T.schedule(task, millis);
			}

			/*************************************************************************/

			Thread.sleep(2000);
			System.out.println("\n\n\nPrinting overall stats");
			System.out.println(GNSNIOTransport.getStats());	
	} catch (IOException e) {
		e.printStackTrace();
	} catch(Exception e) {
		e.printStackTrace();
	}
	}
}
