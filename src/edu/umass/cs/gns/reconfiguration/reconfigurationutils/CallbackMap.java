package edu.umass.cs.gns.reconfiguration.reconfigurationutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.umass.cs.gns.reconfiguration.json.reconfigurationpackets.StopEpoch;

/**
 * @author V. Arun
 */
/*
 * FIXME: May need to add a self-destruct property to entries so that they are
 * automatically removed after some idle time.
 */
public class CallbackMap<NodeIDType> {
	private final HashMap<String, List<StopEpoch<NodeIDType>>> listMap = new HashMap<String, List<StopEpoch<NodeIDType>>>();

	public void addStopNotifiee(StopEpoch<NodeIDType> stopEpoch) {
		if (!this.listMap.containsKey(stopEpoch.getServiceName())) {
			ArrayList<StopEpoch<NodeIDType>> notifiees = new ArrayList<StopEpoch<NodeIDType>>();
			this.listMap.put(stopEpoch.getServiceName(), notifiees);
		}
		this.listMap.get(stopEpoch.getServiceName()).add(stopEpoch);
	}

	public StopEpoch<NodeIDType> notifyStop(String name, int epoch) {
		if (!this.listMap.containsKey(name))
			return null;
		// FIXME: better (but not necessary) to use iterator
		for (StopEpoch<NodeIDType> notifiee : this.listMap.get(name)) {
			if (notifiee.getEpochNumber() - epoch <= 0) {
				this.listMap.get(name).remove(notifiee);
				if (this.listMap.get(name).isEmpty())
					this.listMap.remove(name);
				return notifiee;
			}
		}
		return null;
	}
}
