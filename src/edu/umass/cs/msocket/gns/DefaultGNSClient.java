/**
 * Mobility First - Global Name Resolution Service (GNS)
 * Copyright (C) 2013 University of Massachusetts - Emmanuel Cecchet.
 * Contact: cecchet@cs.umass.edu
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): ______________________.
 */

package edu.umass.cs.msocket.gns;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.UniversalTcpClient;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;



/**
 * exposes gnsClient to GNSCalls and GnsIntegration.
 * This class defines a DefaultGNSClient
 * 
 * @author <a href="mailto:cecchet@cs.umass.edu">Emmanuel Cecchet</a>
 * @version 1.0
 */
public class DefaultGNSClient
{
	public static final int proxyPort			  	= 11989;
	public static final String proxyName 		  	= "ec2-52-4-163-224.compute-1.amazonaws.com";
	
	private static DefaultGNSClient defualtObj    	= null;
	private static final Object lockObj 	      	= new Object();
	
	private static String 	gnsHostPort					    = null;
	private static UniversalTcpClient gnsClient    	= null;
	
	private static GuidEntry myGuidEntry 	      	= null;
	
	
	private DefaultGNSClient()
	{
		try
		{
			String gnsString = KeyPairUtils.getDefaultGns();
			String[] parsed = gnsString.split(":");
			gnsHostPort = parsed[0]+":"+parsed[1];
			System.out.println("gnsHostPort "+gnsHostPort);
			gnsClient = new UniversalTcpClient(parsed[0], Integer.parseInt(parsed[1]), Boolean.parseBoolean(parsed[2]));
			myGuidEntry = KeyPairUtils.getDefaultGuidEntry(gnsHostPort);
			System.out.println("myGuidEntry "+myGuidEntry.getEntityName()+ " "+myGuidEntry.getGuid());
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public static String getDefaultGNSName()
	{
		if(defualtObj == null)
		{
			createSingleton();	
		}
		return gnsHostPort;
	}
	
	public static UniversalTcpClient getGnsClient()
	{
		if(defualtObj == null)
		{
			createSingleton();	
		}
		return gnsClient;
	}
	
	public static GuidEntry getMyGuidEntry()
	{
		if(defualtObj == null)
		{
			createSingleton();	
		}
		return myGuidEntry;
	}
	
	private static void createSingleton()
	{
		synchronized(lockObj)
		{
			if (defualtObj == null)
		    {
				defualtObj = new DefaultGNSClient();
		    }
		}
	}
}