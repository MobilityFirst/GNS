package edu.umass.cs.gns.nio;
/**
@author V. Arun
 */
/* Helps instrument read/write stats in NIOTransport. Used for testing only. */

public class NIOInstrumenter {
	private int totalSent=0;
	private int totalRcvd=0;
	private boolean enabled=true;
	
	NIOInstrumenter() {}
	public void incrSent() {if(enabled) _incrSent();} 
	public void incrRcvd() {if(enabled) _incrRcvd();} 
	public void disable() {enabled=false;}
	public void enable() {enabled=true;}
	public String toString() {
		String s="";
		return s + "Stats: [totalSent = " + totalSent + ", totalRcvd = " + totalRcvd + 
				(totalSent!=totalRcvd ? ", MISSING = " + (totalSent-totalRcvd) : "") + "]";
	}
	private synchronized void _incrSent() {totalSent++;}
	private synchronized void _incrRcvd() {totalRcvd++;}
}
