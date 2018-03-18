package edu.umass.cs.gnsclient.examples.scratch;

import java.util.Arrays;

public class Scratch {
	public static void main(String[] args) {
		String s = "level1.level2.level3";
		System.out.println(Arrays.asList(s.split("\\.")));
	}
}
