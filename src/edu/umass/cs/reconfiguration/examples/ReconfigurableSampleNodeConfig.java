package edu.umass.cs.reconfiguration.examples;

import java.util.HashSet;
import java.util.Set;

import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.reconfiguration.InterfaceReconfigurableNodeConfig;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author V. Arun
 */
public class ReconfigurableSampleNodeConfig extends SampleNodeConfig<Integer>
		implements InterfaceReconfigurableNodeConfig<Integer> {

	private static final int RC_OFFSET = 1000; 
	
	public ReconfigurableSampleNodeConfig() {
		super();
	}

	public ReconfigurableSampleNodeConfig(int defaultPort) {
		super(defaultPort);
	}

	@Override
	public Set<Integer> getActiveReplicas() {
		return this.getNodeIDs();
	}

	/* Add node IDs for RCs that are derived from active node IDs.
	 */
	@Override
	public Set<Integer> getReconfigurators() {
		Set<Integer> actives = this.getNodeIDs();
		Set<Integer> generatedRCs = new HashSet<Integer>();
		for(Integer id : actives) 
			generatedRCs.add(activeToRC(id));
		return generatedRCs;
	}
	
	/* Either id exists as active or exists as RC, i.e., the 
	 * corresponding active exists in the latter case.
	 */
	@Override
	public boolean nodeExists(Integer id) {
		boolean activeExists = super.nodeExists(id);
		boolean RCExists = super.nodeExists(this.RCToActive(id));
		return activeExists || RCExists;
	}

	@Override
	public void localSetup(int nNodes) {
		super.localSetup(nNodes);
	}

	@Override
	public Set<Integer> getValuesFromStringSet(Set<String> strNodes) {
		throw new UnsupportedOperationException("Not supported yet.");
	}

	@Override
	public Set<Integer> getValuesFromJSONArray(JSONArray array)
			throws JSONException {
		throw new UnsupportedOperationException("Not supported yet.");
	}
	
	private Integer activeToRC(int activeID) {
		return activeID + RC_OFFSET;
	}
	private Integer RCToActive(int activeID) {
		return activeID - RC_OFFSET;
	}

}
