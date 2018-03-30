package edu.umass.cs.gnsclient.examples.scratch;

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
		String s = "level1.level2.level3";
		System.out.println(getDottedParents(s));
		System.out.println(s.substring(0, s.lastIndexOf(".")));
		Set<String> set = new HashSet<String>();
		System.out.println(new JSONArray(new HashSet<String>(set)));
		System.out.println("_GNS_ACL.READ_WHITELIST.+ALL+.MD".replaceAll("\\" +
			".MD$","").replaceFirst("\\.[^\\.]*$",""));
		System.out.println(new JSONObject().put("key", (Set<String>)null));
	}
}
