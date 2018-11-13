package edu.umass.cs.gnsclient.examples.scratch;

import edu.umass.cs.gnsclient.client.http.GNSHTTPProxy;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class Scratch {

	public static String getDotParent(String s) {
		return s.replaceFirst("\\.[^\\.]*$", "");
	}

	public static List<String> getDottedParents(String s, ArrayList<String>
		parents) {
		String parent; parents.add(s);
		if (!s.equals(parent = getDotParent(s))) {
			getDottedParents(parent, parents);
		} return parents;
	}

	public static List<String> getDottedParents(String s) {
		return getDottedParents(s, new ArrayList<String>());
	}

	public static void main(String[] args) throws JSONException {
		JSONObject json = new JSONObject().put("key1", "value1").put("key2",
			97);
		String[] array = {"fsdf", "fsdfdsfsfs", "ewrewrw"};
		System.out.println((json.put("key3", array)));
		if(json.get("key3") instanceof JSONArray)
			System.out.println("yes");
		System.out.println(json.get("key3").getClass());
		Properties props = new Properties();
		props.setProperty("key1", "val1");
		props.setProperty("key2", "hello");
		System.out.println(props);

	}
}
