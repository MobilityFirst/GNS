/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.util.TreeSet;
import java.util.Set;

import edu.umass.cs.nio.nioutils.SampleNodeConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;

import org.json.JSONArray;
import org.json.JSONException;

/**
 * @author V. Arun
 */
public class ReconfigurableSampleNodeConfig extends SampleNodeConfig<Integer>
		implements ReconfigurableNodeConfig<Integer> {

	private static final int AR_OFFSET = 100;
	private static final int RC_OFFSET = 1100;

	private int numActives = 0;
	private int numRCs = 0;

	/**
	 * 
	 */
	public ReconfigurableSampleNodeConfig() {
		super();
	}

	/**
	 * @param defaultPort
	 */
	public ReconfigurableSampleNodeConfig(int defaultPort) {
		super(defaultPort);
	}

	@Override
	public Set<Integer> getActiveReplicas() {
		Set<Integer> allNodes = this.getNodeIDs();
		Set<Integer> actives = new TreeSet<Integer>();
		int count = 0;
		for (Integer id : allNodes) {
			actives.add(numberToActive(id));
			if (++count == this.numActives)
				break;
		}
		return actives;

	}

	/*
	 * Add node IDs for RCs that are derived from active node IDs.
	 */
	@Override
	public Set<Integer> getReconfigurators() {
		Set<Integer> allNodes = this.getNodeIDs();
		Set<Integer> generatedRCs = new TreeSet<Integer>();
		int count = 0;
		for (Integer id : allNodes) {
			generatedRCs.add(numberToRC(id));
			if (++count == this.numRCs)
				break;
		}
		return generatedRCs;
	}

	/*
	 * Either id exists as active or exists as RC, i.e., the corresponding
	 * active exists in the latter case.
	 */
	@Override
	public boolean nodeExists(Integer id) {
		boolean activeExists = super.nodeExists(this.activeToNumber(id));
		boolean RCExists = super.nodeExists(this.RCToNumber(id));
		return activeExists || RCExists;
	}

	@Override
	public void localSetup(int nNodes) {
		super.localSetup(nNodes);
		this.numActives = nNodes;
		this.numRCs = nNodes;
	}

	/**
	 * @param numActives
	 * @param numRCs
	 */
	protected void localSetupARRC(int numActives, int numRCs) {
		super.localSetup(Math.max(numActives, numRCs));
		this.numActives = numActives;
		this.numRCs = numRCs;
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

	private Integer numberToRC(int number) {
		return number + RC_OFFSET;
	}

	private Integer RCToNumber(int number) {
		return number - RC_OFFSET;
	}
	private Integer numberToActive(int number) {
		return number + AR_OFFSET;
	}

	private Integer activeToNumber(int number) {
		return number - AR_OFFSET;
	}

}
