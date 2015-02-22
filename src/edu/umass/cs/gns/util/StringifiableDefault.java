package edu.umass.cs.gns.util;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.gns.nio.JSONPacket;

public class StringifiableDefault<ObjectType> implements Stringifiable<ObjectType> {
	Object seedObj;
	public StringifiableDefault(Object obj) {
		this.seedObj = obj;
	}
	@Override
	public ObjectType valueOf(String strValue) {
		if(seedObj instanceof Integer) return (ObjectType) Integer.valueOf(strValue);
		else if(seedObj instanceof String) return (ObjectType) strValue;
		else if(seedObj instanceof InetSocketAddress) return (ObjectType) toInetSocketAddress(strValue);
		else if(seedObj instanceof Stringifiable<?>) return (ObjectType)((Stringifiable<?>)seedObj).valueOf(strValue);
		else return null;
	}

	@Override
	public Set<ObjectType> getValuesFromStringSet(Set<String> strValues) {
		Set<ObjectType> set = new HashSet<ObjectType>();
		for(String str : strValues) set.add(valueOf(str));
		return set;
	}
	@Override
	public Set<ObjectType> getValuesFromJSONArray(JSONArray array)
			throws JSONException {
		Set<ObjectType> set = new HashSet<ObjectType>();
		for(int i=0; i<array.length(); i++) set.add(valueOf(array.getString(i)));
		return set;
	}

	private InetSocketAddress toInetSocketAddress(String string) {
		String[] tokens = string.split(":");
		if(tokens.length!=2) return null;
		InetAddress IP = null;
		int port = -1;
		try {
			IP = InetAddress.getByName(tokens[0].replaceAll("[^0-9.]*", ""));
			port = Integer.valueOf(tokens[1]);
		} catch (UnknownHostException | NumberFormatException e) {
			e.printStackTrace();
			return null;
		}
		return new InetSocketAddress(IP, port);
	}
	public static void main(String[] args) {
		StringifiableDefault<Integer> unstringer1 = new StringifiableDefault<Integer>(0);
		StringifiableDefault<String> unstringer2 = new StringifiableDefault<String>("");
		System.out.println(unstringer1.valueOf("43"));
		System.out.println(unstringer2.valueOf("abc"));
		Object obj = null;
		try {
			obj = (unstringer1.valueOf("qwe"));
		} catch(NumberFormatException e) {}
		assert(obj==null);
	}
}
