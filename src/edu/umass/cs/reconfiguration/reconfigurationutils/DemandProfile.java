/*
 * Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun
 */
package edu.umass.cs.reconfiguration.reconfigurationutils;

import java.net.InetAddress;
import java.util.ArrayList;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.utils.Util;

/**
 * @author V. Arun
 * 
 *         This class maintains the deman profile for a single name and returns
 *         it into a JSONObject via its getStats() method.
 */
public class DemandProfile extends AbstractDemandProfile {
	protected enum Keys {
		SERVICE_NAME, STATS, RATE, NUM_REQUESTS, NUM_TOTAL_REQUESTS
	};

	private static final int DEFAULT_NUM_REQUESTS = 1;
	private static final long MIN_RECONFIGURATION_INTERVAL = 000;
	private static final long MIN_REQUESTS_BEFORE_RECONFIGURATION = DEFAULT_NUM_REQUESTS;

	private double interArrivalTime = 0.0;
	private long lastRequestTime = 0;
	private int numRequests = 0;
	private int numTotalRequests = 0;
	private DemandProfile lastReconfiguredProfile = null;

	/**
	 * @param name
	 */
	public DemandProfile(String name) {
		super(name);
	}

	/**
	 * Deep copy constructor.
	 * 
	 * @param dp
	 */
	public DemandProfile(DemandProfile dp) {
		super(dp.name);
		this.interArrivalTime = dp.interArrivalTime;
		this.lastRequestTime = dp.lastRequestTime;
		this.numRequests = dp.numRequests;
		this.numTotalRequests = dp.numTotalRequests;
	}

	/**
	 * @param json
	 * @throws JSONException
	 */
	public DemandProfile(JSONObject json) throws JSONException {
		super(json.getString(Keys.SERVICE_NAME.toString()));
		this.interArrivalTime = 1.0 / json.getDouble(Keys.RATE.toString());
		this.numRequests = json.getInt(Keys.NUM_REQUESTS.toString());
		this.numTotalRequests = json.getInt(Keys.NUM_TOTAL_REQUESTS.toString());
	}

	public static DemandProfile createDemandProfile(String name) {
		return new DemandProfile(name);
	}

	/*
	 * FIXME: Ignoring sender argument for now. Need to use it to develop a
	 * demand geo-distribution profile.
	 */
	@Override
	public void register(Request request, InetAddress sender,
			InterfaceGetActiveIPs nodeConfig) {
		if (!request.getServiceName().equals(this.name))
			return;
		this.numRequests++;
		this.numTotalRequests++;
		long iaTime = 0;
		if (lastRequestTime > 0) {
			iaTime = System.currentTimeMillis() - this.lastRequestTime;
			this.interArrivalTime = Util
					.movingAverage(iaTime, interArrivalTime);
		} else
			lastRequestTime = System.currentTimeMillis(); // initialization
	}

	/**
	 * @return Request rate.
	 */
	public double getRequestRate() {
		return this.interArrivalTime > 0 ? 1.0 / this.interArrivalTime
				: 1.0 / (this.interArrivalTime + 1000);
	}

	/**
	 * @return Number of requests since the previous report.
	 */
	public double getNumRequests() {
		return this.numRequests;
	}

	/**
	 * @return Number of total requests.
	 */
	public double getNumTotalRequests() {
		return this.numTotalRequests;
	}

	@Override
	public boolean shouldReport() {
		if (getNumRequests() >= DEFAULT_NUM_REQUESTS)
			return true;
		return false;
	}

	@Override
	public JSONObject getStats() {
		JSONObject json = new JSONObject();
		try {
			json.put(Keys.SERVICE_NAME.toString(), this.name);
			json.put(Keys.RATE.toString(), getRequestRate());
			json.put(Keys.NUM_REQUESTS.toString(), getNumRequests());
			json.put(Keys.NUM_TOTAL_REQUESTS.toString(), getNumTotalRequests());
		} catch (JSONException je) {
			je.printStackTrace();
		}
		return json;
	}

	@Override
	public void reset() {
		this.interArrivalTime = 0.0;
		this.lastRequestTime = 0;
		this.numRequests = 0;
	}

	@Override
	public DemandProfile clone() {
		return new DemandProfile(this);
	}

	@Override
	public void combine(AbstractDemandProfile dp) {
		DemandProfile update = (DemandProfile) dp;
		this.lastRequestTime = Math.max(this.lastRequestTime,
				update.lastRequestTime);
		this.interArrivalTime = Util.movingAverage(update.interArrivalTime,
				this.interArrivalTime, update.getNumRequests());
		this.numRequests += update.numRequests; // this number is not meaningful
												// at RC
		this.numTotalRequests += update.numTotalRequests;
	}

	@Override
	public ArrayList<InetAddress> shouldReconfigure(
			ArrayList<InetAddress> curActives, InterfaceGetActiveIPs nodeConfig) {
		if (this.lastReconfiguredProfile != null) {
			if (System.currentTimeMillis()
					- this.lastReconfiguredProfile.lastRequestTime < MIN_RECONFIGURATION_INTERVAL)
				return null;
			if (this.numTotalRequests
					- this.lastReconfiguredProfile.numTotalRequests < MIN_REQUESTS_BEFORE_RECONFIGURATION)
				return null;
		}
		/**
		 * This example simply returns curActives as this policy trivially
		 * reconfigures to the same set of locations after every call. In
		 * general, AbstractDemandProfile implementations should return a new
		 * list different from curActives.
		 */
		return curActives;
	}

	@Override
	public void justReconfigured() {
		this.lastReconfiguredProfile = this.clone();
	}
}
