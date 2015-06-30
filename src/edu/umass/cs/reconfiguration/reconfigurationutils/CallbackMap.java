package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.umass.cs.reconfiguration.reconfigurationpackets.StopEpoch;

/**
 * @author V. Arun
 * @param <NodeIDType>
 * 
 *            FIXME: May need to add a self-destruct property to entries so that
 *            they are automatically removed after some idle time.
 */
public class CallbackMap<NodeIDType> {
	private final HashMap<String, List<StopEpoch<NodeIDType>>> listMap = new HashMap<String, List<StopEpoch<NodeIDType>>>();

	/**
	 * @param stopEpoch
	 * @return True as specified by {@link List#add(Object)}.
	 */
	public boolean addStopNotifiee(StopEpoch<NodeIDType> stopEpoch) {
		if (!this.listMap.containsKey(stopEpoch.getServiceName())) {
			ArrayList<StopEpoch<NodeIDType>> notifiees = new ArrayList<StopEpoch<NodeIDType>>();
			this.listMap.put(stopEpoch.getServiceName(), notifiees);
		}
		return this.listMap.get(stopEpoch.getServiceName()).add(stopEpoch);
	}

	/**
	 * @param name
	 * @param epoch
	 * @return StopEpoch requiring acknowledgment.
	 */
	public StopEpoch<NodeIDType> notifyStop(String name, int epoch) {
		if (!this.listMap.containsKey(name))
			return null;
		StopEpoch<NodeIDType> notifiee = null;
		for (Iterator<StopEpoch<NodeIDType>> notifieeIter = this.listMap.get(
				name).iterator(); notifieeIter.hasNext();) {
			notifiee = notifieeIter.next();
			if (notifiee.getEpochNumber() - epoch > 0)
				continue;
			// else
			notifieeIter.remove();
			break;
		}
		if (this.listMap.get(name).isEmpty())
			this.listMap.remove(name);
		return notifiee;
	}
}
