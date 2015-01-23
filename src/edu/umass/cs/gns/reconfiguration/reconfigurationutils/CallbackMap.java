package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.HashMap;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StopEpoch;

/**
 * @author V. Arun
 */
/*
 * FIXME: May need to add a self-destruct property to entries so that they are automatically removed
 * after some idle time.
 */
public class CallbackMap<NodeIDType> {
	private final HashMap<String, BasicReconfigurationPacket<NodeIDType>> map = new HashMap<String, BasicReconfigurationPacket<NodeIDType>>();

	public void put(StopEpoch<NodeIDType> stopEpoch) {
		if (!this.map.containsKey(stopEpoch.getServiceName())
				|| (this.map.get(stopEpoch.getServiceName())).getEpochNumber()
						- stopEpoch.getEpochNumber() < 0) {
			this.map.put(stopEpoch.getServiceName(), stopEpoch);
		}
	}

	public BasicReconfigurationPacket<NodeIDType> get(String name) {
		return this.map.get(name);
	}

	public BasicReconfigurationPacket<NodeIDType> remove(String name) {
		return this.map.remove(name);
	}
}
