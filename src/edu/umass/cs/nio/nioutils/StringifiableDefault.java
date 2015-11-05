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
package edu.umass.cs.nio.nioutils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.nio.interfaces.Stringifiable;

/**
 * @author arun
 *
 * @param <ObjectType>
 */
public class StringifiableDefault<ObjectType> implements
		Stringifiable<ObjectType> {

	Object seedObj;

	/**
	 * @param obj
	 *            Seed object used to infer return type for
	 *            {@link #valueOf(String)} method.
	 */
	public StringifiableDefault(Object obj) {
		this.seedObj = obj;
	}

	/**
	 * 
	 * @param strValue
	 * @return Returns the ObjectType corresponding to {@code strValue}.
	 */
	// all types explicitly checked, so okay to suppress warnings
	@SuppressWarnings("unchecked")
	@Override
	public ObjectType valueOf(String strValue) {
		if (seedObj instanceof Integer)
			return (ObjectType) Integer.valueOf(strValue);
		else if (seedObj instanceof String)
			return (ObjectType) strValue;
		else if (seedObj instanceof InetSocketAddress)
			return (ObjectType) toInetSocketAddress(strValue);
		else if (seedObj instanceof Stringifiable<?>)
			return (ObjectType) ((Stringifiable<?>) seedObj).valueOf(strValue);
		else
			return null;
	}

	@Override
	public Set<ObjectType> getValuesFromStringSet(Set<String> strValues) {
		Set<ObjectType> set = new HashSet<ObjectType>();
		for (String str : strValues)
			set.add(valueOf(str));
		return set;
	}

	@Override
	public Set<ObjectType> getValuesFromJSONArray(JSONArray array)
			throws JSONException {
		Set<ObjectType> set = new HashSet<ObjectType>();
		for (int i = 0; i < array.length(); i++)
			set.add(valueOf(array.getString(i)));
		return set;
	}

	private InetSocketAddress toInetSocketAddress(String string) {
		String[] tokens = string.split(":");
		if (tokens.length != 2)
			return null;
		String addressPart = tokens[0];
		// deals with ec2-23-21-160-80.compute-1.amazonaws.com/10.154.130.201
		if (addressPart.indexOf("/") != -1)
			addressPart = addressPart.substring(addressPart.indexOf("/") + 1);

		InetAddress IP = null;
		int port = -1;
		try {
			IP = InetAddress.getByName(addressPart.replaceAll("[^0-9.]*", ""));
			port = Integer.valueOf(tokens[1]);
		} catch (UnknownHostException | NumberFormatException e) {
			NIOTransport.getLogger().severe(
					"StringifiableDefault:: Error while converting " + string
							+ " to InetSocketAddress: " + e);
			e.printStackTrace();
			return null;
		}
		return new InetSocketAddress(IP, port);
	}

	static class Main {
		static void main(String[] args) {
			StringifiableDefault<Integer> unstringer1 = new StringifiableDefault<Integer>(
					0);
			StringifiableDefault<String> unstringer2 = new StringifiableDefault<String>(
					"");
			System.out.println(unstringer1.valueOf("43"));
			System.out.println(unstringer2.valueOf("abc"));
			Object obj = null;
			try {
				obj = (unstringer1.valueOf("qwe"));
			} catch (NumberFormatException e) {
			}
			assert (obj == null);
		}
	}
}
