package edu.umass.cs.gns.reconfiguration;
/**
@author V. Arun
 */
public interface InterfaceReplicableRequest extends InterfaceRequest {
	public boolean needsCoordination();
	public void setNeedsCoordination(boolean b);
}
