package edu.umass.cs.gns.util;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;

public class StringifiableDefault<ObjectType> implements Stringifiable<ObjectType> {
	Object seedObj;
	public StringifiableDefault(Object obj) {
		this.seedObj = obj;
	}
	@Override
	public ObjectType valueOf(String strValue) {
		if(seedObj instanceof Integer) return (ObjectType) Integer.valueOf(strValue);
		else if(seedObj instanceof String) return (ObjectType) strValue;
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
