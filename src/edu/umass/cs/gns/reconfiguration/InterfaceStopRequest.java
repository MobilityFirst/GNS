package edu.umass.cs.gns.reconfiguration;
/**
@author V. Arun
 */
public interface InterfaceStopRequest extends InterfaceRequest {
	public int getEpochNumber();
	public boolean isStop();
}
