package edu.umass.cs.gnsserver.activecode.prototype.utils;

import java.io.IOException;
import java.net.InetAddress;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

/**
 * @author gaozy
 *
 */
public class GeoIPUtils {

	/**
	 * This method is used for test, whether a static method can be called by active code  
	 * @param ip
	 * @param dbReader
	 * @return a GeoIP response
	 */
	public static CityResponse getLocation_City(String ip, DatabaseReader dbReader) {		
		try {
			InetAddress ipAddress = InetAddress.getByName(ip);
			CityResponse response = dbReader.city(ipAddress);			
			return response;
			
		} catch (IOException | GeoIp2Exception e) {
			return null;
		}		
	}
}
