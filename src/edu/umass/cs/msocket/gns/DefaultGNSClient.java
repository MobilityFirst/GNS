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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Random;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.util.GuidEntry;
import edu.umass.cs.gnsclient.client.util.GuidUtils;
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
	public static final String GNS_CONFIG_DIRNAME	= "gnsConfigFiles";
	
	public static final int PROXY_PORT			  	= 3033;
	public static final String PROXY_NAME 		  	= "ananas.cs.umass.edu";
	
	private static DefaultGNSClient defualtObj    	= null;
	private static final Object lockObj 	      	= new Object();
	
	//private static String 	gnsHostPort				= null;
	private static GNSClient gnsClient    			= null;
	
	private static GuidEntry myGuidEntry 	      	= null;
	
	
	private DefaultGNSClient()
	{
		try
		{
			checkAndCreateConfigFiles();
			
			// setting the properties
			Properties props = System.getProperties();
			props.setProperty("gigapaxosConfig", 
					GNS_CONFIG_DIRNAME+"/"+"gnsclient.msocket.properties");
			
			props.setProperty("javax.net.ssl.trustStorePassword", "qwerty");
			props.setProperty("javax.net.ssl.trustStore", 
					GNS_CONFIG_DIRNAME +"/"+"trustStore.jks");
			props.setProperty("javax.net.ssl.keyStorePassword", "qwerty");
			props.setProperty("javax.net.ssl.keyStore", 
					GNS_CONFIG_DIRNAME+"/"+"keyStore.jks");
			
			gnsClient = new GNSClient();
			
			myGuidEntry = KeyPairUtils.getDefaultGuidEntry(gnsClient.getGNSProvider());
			
			if( myGuidEntry == null )
			{
				Random rand = new Random(System.currentTimeMillis());
				int randInt = Math.abs(rand.nextInt());
				String accountAlias = "admin"+randInt+"@gns.name";
				
				// create a random account GUID;
				myGuidEntry = GuidUtils.lookupOrCreateAccountGuid
						(gnsClient, accountAlias, "password", true);
				
				KeyPairUtils.setDefaultGuidEntry(gnsClient.getGNSProvider(), accountAlias);
			}
			
//			System.out.println("myGuidEntry "+myGuidEntry.getEntityName()
//													+ " "+myGuidEntry.getGuid());
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void checkAndCreateConfigFiles() throws IOException
	{
		// create a directory if it is not there.
		File directoryFile = new File("gnsConfigFiles");
        if ( !directoryFile.exists() )
        {
            if ( directoryFile.mkdir() )
            {
            	// gnsclient.msocket.properties
            	String gigapaxosPropFile = "gnsclient.msocket.properties";
         
            	writeAFileFromJarToLocalDirByteSteam(gigapaxosPropFile);
            	
            	
            	// keystore.jks file
            	String keyStoreFile = "keyStore.jks";
            	writeAFileFromJarToLocalDirByteSteam(keyStoreFile);
            	
            	
            	// trustStore.jks file
            	String trustStoreFile = "trustStore.jks";
            	writeAFileFromJarToLocalDirByteSteam(trustStoreFile);
            	
            }
            else
            {
                throw new IOException("Failed to create the gnsConfigFiles directory");
            }
        }
        else
        {
        	// directory already there, check for files. Files should also be there.
        	// FIXME: may be we should checks if all files are there or create them again.
        }
	}
	
//	java.util.logging.SimpleFormatter.format=%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS [%4$s] %5$s%6$s%n
//			.level=ALL
//			handlers=java.util.logging.FileHandler
//			java.util.logging.FileHandler.level=INFO
//			java.util.logging.ConsoleHandler.level=WARNING
//			java.util.logging.FileHandler.pattern = /tmp/gigapaxos.log
//			java.util.logging.FileHandler.formatter = java.util.logging.SimpleFormatter

	
	
	private void writeAFileFromJarToLocalDir(String fileName)
	{
    	File configFile 
    		= new File(GNS_CONFIG_DIRNAME+"/"+fileName);
    	
    	FileWriter fw = null;
    	BufferedWriter bw = null;
    	BufferedReader reader = null;
		
    	try
    	{
	    	// if file doesn't exists, then create it
			if ( !configFile.exists() )
			{
				configFile.createNewFile();
			}
			
	    	fw = new FileWriter(configFile.getAbsoluteFile());
			bw = new BufferedWriter(fw);
			
			
			InputStream in = getClass().getResourceAsStream("/"+fileName);
			reader = new BufferedReader(new InputStreamReader(in));
			
//	    	reader = new BufferedReader( new InputStreamReader(
//	    		    this.getClass().getResourceAsStream(fileName)) );
	    	
	    	String sCurrentLine;
	    	
	    	while ( (sCurrentLine = reader.readLine()) != null ) 
	    	{
	    		bw.write(sCurrentLine + "\n");
			}
    	}
    	catch(IOException ioex)
    	{
    		ioex.printStackTrace();
    	}
    	finally
    	{
    		if( bw != null)
    		{
    			try 
    			{
					bw.close();
				} catch (IOException e) 
    			{
					e.printStackTrace();
				}
    		}
    		
    		if( reader != null )
    		{
    			try 
    			{
					reader.close();
				} catch (IOException e) 
    			{
					e.printStackTrace();
				}
    		}
    		
    		if( fw != null )
    		{
    			try
    			{
					fw.close();
				} catch (IOException e)
    			{
					e.printStackTrace();
				}
    		}
    	}
    	
	}
	
	
	private void writeAFileFromJarToLocalDirByteSteam(String fileName)
	{	
    	FileOutputStream writerOut = null;
    	
    	InputStream readerIn = null;
    	
    	byte[] byteArrayBuf = new byte[1000];
    	
    	try
    	{
	    	// if file doesn't exists, then create it
//			if ( !configFile.exists() )
//			{
//				configFile.createNewFile();
//			}
			
			writerOut = new FileOutputStream(GNS_CONFIG_DIRNAME+"/"+fileName);
			
			
			readerIn = getClass().getResourceAsStream("/"+fileName);
			//reader = new BufferedReader(new InputStreamReader(in));
			
//	    	reader = new BufferedReader( new InputStreamReader(
//	    		    this.getClass().getResourceAsStream(fileName)) );
	    	int numRead = -1;
			do
	    	{
				numRead = readerIn.read(byteArrayBuf);
				if(numRead > 0)
				{
					writerOut.write(byteArrayBuf);
				}
	    		
	    	}while(numRead != -1);
    	}
    	catch(IOException ioex)
    	{
    		ioex.printStackTrace();
    	}
    	finally
    	{
    		if( writerOut != null)
    		{
    			try 
    			{
    				writerOut.close();
				} catch (IOException e) 
    			{
					e.printStackTrace();
				}
    		}
    		
    		if( writerOut != null )
    		{
    			try 
    			{
    				writerOut.close();
				} catch (IOException e) 
    			{
					e.printStackTrace();
				}
    		}
    	}
    	
	}
	
	public static String getDefaultGNSName()
	{
		if(defualtObj == null)
		{
			createSingleton();	
		}
		assert(gnsClient != null);
		return gnsClient.getGNSProvider();
	}
	
	public static GNSClient getGnsClient()
	{
		if(defualtObj == null)
		{
			createSingleton();	
		}
		assert(gnsClient != null);
		return gnsClient;
	}
	
	public static GuidEntry getMyGuidEntry()
	{
		if(defualtObj == null)
		{
			createSingleton();	
		}
		assert(myGuidEntry != null);
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
	
	
	public static void main(String [] args)
	{
		GNSClient gnsClient = DefaultGNSClient.getGnsClient();
		System.out.println("GNS name "+gnsClient.getGNSProvider());
	}
}