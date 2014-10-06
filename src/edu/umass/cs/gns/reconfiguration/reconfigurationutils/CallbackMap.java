package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.HashMap;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StopEpoch;

/**
@author V. Arun
 */
/* FIXME: May need to add a self-destruct property to entries
 * so that they are automatically removed after some idle
 * time.
 */
public class CallbackMap<NodeIDType> {
	private final HashMap<String,StopEpoch<NodeIDType>> map = new HashMap<String,StopEpoch<NodeIDType>>();
	
	public void put(StopEpoch<NodeIDType> stopEpoch) {
		if(!this.map.containsKey(stopEpoch.getServiceName()) || 
				(this.map.get(stopEpoch.getServiceName())).getEpochNumber() - stopEpoch.getEpochNumber() < 0) {
			this.map.put(stopEpoch.getServiceName(), stopEpoch);
		}
	}
	public StopEpoch<NodeIDType> get(String name) {
		return (StopEpoch<NodeIDType>)this.map.get(name);
	}
	public StopEpoch<NodeIDType> remove(String name) {
		return (StopEpoch<NodeIDType>)this.map.remove(name);
	}
}
