package edu.umass.cs.gns.util;

import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */
public interface Reportable<NodeIDType> {
	public JSONObject getStats(); // default, all stats
	public Set<NodeIDType> getRecipients(); // default, all stats
	public JSONObject getStats(String statID); // specific stat reporting
	public Set<NodeIDType> getRecipients(String statID); // specific stat's destinations
	public JSONMessenger<NodeIDType> getJSONMessenger(); // need to supply this as we want to reuse NIO
}
