package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;

/**
 * @author gaozy
 *
 */
public class SingleClient implements Runnable{
	// the total number of requests need to be sent
	private int numReq;
    private GuidEntry entry;
    private UniversalTcpClient client;
    private boolean malicious;
    
	/**
	 * @param client
	 * @param entry
	 * @param malicious
	 */
	public SingleClient(UniversalTcpClient client, GuidEntry entry, boolean malicious){
		this.client = client;
		this.entry = entry;
		this.malicious = malicious;
		if (malicious){
			this.numReq = MessageStats.DURATION*1000/MessageStats.MAL_INTERVAL/MessageStats.DEPTH;
		} else{
			this.numReq = MessageStats.DURATION*1000/MessageStats.INTERVAL;
		}
		numReq = 20;
		System.out.println("Start Sending "+numReq+" requests...");
	}
	
	public void run(){
		for (int i=0; i<numReq; i++){
			long start = System.nanoTime();
			try{
				client.fieldRead(entry, "nextGuid");
				//System.out.println("query result is "+result);
			}catch (Exception e){
				e.printStackTrace();
			}
			long eclapsed = System.nanoTime() - start;			
			
			if (malicious){
				MessageStats.mal_request.add(eclapsed);
			}else{
				MessageStats.latency.add(eclapsed);
			}
			
		}
	}
}