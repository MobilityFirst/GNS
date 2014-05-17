package edu.umass.cs.gns.replicaCoordination.multipaxos.scratch;


/**
@author V. Arun
 */
public class Scratchpad {

	private Integer processing = new Integer(0);
	
	private void setProcessing(boolean b) {
		synchronized(processing) {
			if(b) processing++; else processing--; 
			processing.notify();
		}
	}
	private boolean getProcessing() {
		synchronized(processing) {
			return processing>0;
		}
	}
	
	public static void main(String[] args) {
		Scratchpad sp = new Scratchpad();
		sp.setProcessing(true);
		sp.setProcessing(false);
	
		Scratchpad sc2 = new Scratchpad();
		System.out.println(sc2.getProcessing());
	}
}
