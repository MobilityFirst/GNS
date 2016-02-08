package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;

public class SingleClient implements Runnable{
	// the total number of requests need to be sent
	private int numReq;
    private GuidEntry entry;
    private UniversalTcpClient client;
    private boolean malicious;
    
	public SingleClient(UniversalTcpClient client, GuidEntry entry, boolean malicious){
		this.client = client;
		this.entry = entry;
		this.malicious = malicious;
		if (malicious){
			this.numReq = CapacityTestSequentialClient.DURATION*1000/CapacityTestChainClient.MAL_INTERVAL/CapacityTestChainClient.DEPTH;
		} else{
			this.numReq = CapacityTestSequentialClient.DURATION*1000/CapacityTestChainClient.INTERVAL;
		} 
	}
	
	public void run(){
		for (int i=0; i<numReq; i++){
			long start = System.nanoTime();
			try{
				String result = client.fieldRead(entry, "nextGuid");
				System.out.println("query result is "+result);
			}catch (Exception e){
				e.printStackTrace();
			}
			long eclapsed = System.nanoTime() - start;			
			
			if (malicious){
				CapacityTestChainClient.mal_request.add(eclapsed);
			}else{
				CapacityTestChainClient.latency.add(eclapsed);
			}
			
		}
	}
}