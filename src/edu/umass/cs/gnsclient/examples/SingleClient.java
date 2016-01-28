package edu.umass.cs.gnsclient.examples;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;

public class SingleClient implements Runnable{
	// the total number of requests need to be sent
	private int numReq;
    private GuidEntry entry;
    private UniversalTcpClient client;
    
	public SingleClient(UniversalTcpClient client, GuidEntry entry, boolean malicious){
		this.client = client;
		this.entry = entry;
		if (malicious){
			this.numReq = CapacityTestSequentialClient.DURATION*1000/CapacityTestSequentialClient.MAL_INTERVAL;
		} else{
			this.numReq = CapacityTestSequentialClient.DURATION*1000/CapacityTestSequentialClient.INTERVAL;
		}
		//System.out.println("There are "+numReq+" requests.");
	}
	
	public void run(){
		for (int i=0; i<numReq; i++){
			long start = System.nanoTime();
			try{
				client.fieldRead(entry, "hi");
			}catch (Exception e){
				e.printStackTrace();
			}
			long eclapsed = System.nanoTime() - start;

			CapacityTestSequentialClient.latency.add(eclapsed);
			
			/*
			if (malicious){
				CapacityTestSequentialClient.mal_request.add(eclapsed);
			}else{
				CapacityTestSequentialClient.latency.add(eclapsed);
			}
			*/
		}
	}
}