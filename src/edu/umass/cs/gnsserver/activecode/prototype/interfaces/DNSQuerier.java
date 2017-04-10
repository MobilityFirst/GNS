package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;

/**
 * This interface defines the methods that will be used
 * by workers to make DNS-related queries, e.g., GeoIP.
 * 
 * @author gaozy
 *
 */
public interface DNSQuerier {
	
	/**
	 * Resolve a list of IP addresses to geographic locations
	 * @param ipList 
	 * @return a JSON which contains the location information for each IP address, 
	 * 		   we recommend the returned is formatted as {ip1: {"latitude":lat1, "longitude":lng1},...}
	 * 
	 * @throws ActiveException 
	 */
	public ScriptObjectMirror getLocations(ScriptObjectMirror ipList) throws ActiveException;
		
}
