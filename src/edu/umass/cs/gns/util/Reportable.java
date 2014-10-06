package edu.umass.cs.gns.util;

import java.util.Set;

import org.json.JSONObject;

import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nsdesign.nodeconfig.NodeId;

/**
@author V. Arun
 */
public interface Reportable {
	public JSONObject getStats(); // default, all stats
	public Set<NodeId<String>> getRecipients(); // default, all stats
	public JSONObject getStats(String statID); // specific stat reporting
	public Set<Integer> getRecipients(String statID); // specific stat's destinations
	public JSONMessenger getJSONMessenger(); // need to supply this as we want to reuse NIO
}
