package edu.umass.cs.msocket.gns;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.client.util.KeyPairUtils;
import edu.umass.cs.gnsclient.exceptions.GnsException;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.msocket.common.GnsConstants;


public class GNSCalls
{
	// should be same as defined in context service code
	public static final String NOTIFICATION_SET               	= "NOTIFICATION_SET";
	
	//private static final String defaultGns = KeyPairUtils.getDefaultGnsFromPreferences();
	//public static final UniversalTcpClient gnsClient 
	//			= new UniversalTcpClient(defaultGns.split(":")[0], Integer.parseInt(defaultGns.split(":")[1]));
	
	//private static final GuidEntry myGuidEntry = KeyPairUtils.getDefaultGuidEntryFromPreferences(defaultGns);
	
	private static final Logger log = Logger.getLogger(GNSCalls.class.getName());
	
	public static void updateMyLocationInGns(String localAlias, double Lat, double Longi)
	{
		try
		{
			log.trace("Looking for entity " + localAlias + " GUID and certificates...");
			
			GuidEntry myGuid = KeyPairUtils.getGuidEntry(DefaultGNSClient.getDefaultGNSName(), localAlias);
		    /*
		     * Take a lock on the GNS connection object to prevent concurrent queries to
		     * the GNS on the same connection as the library is not thread-safe from
		     * that standpoint.
		     */
		     log.trace("Updating location for " + localAlias + " to " + Longi+" " + Lat);
		     DefaultGNSClient.getGnsClient().setLocation(myGuid, Longi, Lat);
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/**
	 * Takes alias of group, which is query and reads group members
	 * , if reading from gns fails then request is sent to contextservice
	 * @param query
	 * @return guids of group members
	 * @throws GnsException
	 * @throws IOException
	 * @throws UnsupportedEncodingException
	 */
	public static JSONArray readGroupMembers(String query, String groupGUID) throws UnsupportedEncodingException, IOException, GnsException
	{
		JSONArray grpMem = null;
		//String queryHash = getSHA1(query);
		//String guidString = gnsClient.lookupGuid(queryHash);
		// group should be read by all, atleast for now
		grpMem = DefaultGNSClient.getGnsClient().groupGetMembers(groupGUID, DefaultGNSClient.getMyGuidEntry());
		return grpMem;
	}
	
	public static String getGroupGUID(String query) throws UnsupportedEncodingException, IOException, GnsException
	{
		String queryHash = getSHA1(query);
		String guidString = DefaultGNSClient.getGnsClient().lookupGuid(queryHash);
		return guidString;
	}
	
	public static void writeKeyValue(String localName,
		      String key, double value) throws Exception
	{
		    String GNSKey   = key;
		    double GNSValue = value;
		    
		    try
		    {
				log.trace("Looking for entity " + localName + " GUID and certificates...");
				
				GuidEntry myGuid = KeyPairUtils.getGuidEntry(DefaultGNSClient.getDefaultGNSName(), localName);
				
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			      if (myGuid == null)
			      {
			        log.trace("No keys found for service " + localName + ". Generating new GUID and keys");
			        // Create a new GUID
			        myGuid = DefaultGNSClient.getGnsClient().guidCreate(DefaultGNSClient.getMyGuidEntry(), localName);
			        
			        // storing alias in gns record, need it to find it when we have GUID
			        // from group members
			        DefaultGNSClient.getGnsClient().fieldCreateList(myGuid.getGuid(), GnsConstants.ALIAS_FIELD, new JSONArray().put(localName), myGuid);
			        
			        JSONArray valJSONArray = new JSONArray();
			        valJSONArray.put(GNSValue);
			        valJSONArray.put(System.currentTimeMillis());
			        
			        DefaultGNSClient.getGnsClient()
			        	.fieldReplaceOrCreateList(myGuid.getGuid(), GNSKey, valJSONArray, myGuid);
			      }
			      else
			      {
			    	  System.out.println("GUID already registered, just replacing the field value "
			            + localName + " values " + GNSValue);
			    	  
			    	  JSONArray valJSONArray = new JSONArray();
				      valJSONArray.put(GNSValue);
				      valJSONArray.put(System.currentTimeMillis());
				        
			    	  long start = System.currentTimeMillis();
			        
			    	  DefaultGNSClient.getGnsClient()
		        	.fieldReplaceOrCreateList(myGuid.getGuid(), GNSKey, valJSONArray, myGuid);
			        long end = System.currentTimeMillis();
			        
			        System.out.println("writeKeyValue: fieldReplaceOrCreateList "+(end-start)+" curr Time "+System.currentTimeMillis());
			        
			        // JSONArray arr= gnsClient.fieldRead(myGuid.getGuid(), GNSKey, myGuid);
			        //System.out.println("read result "+gnsClient.lookupGuidRecord(myGuid.getGuid()));
			      }
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
	}
	
	public static void writeKeyValue(String localName,
		      String key, String value) throws Exception
	{
		    String GNSKey   = key;
		    String GNSValue = value;
		    
		    try
		    {
				log.trace("Looking for entity " + localName + " GUID and certificates...");
				
				GuidEntry myGuid = KeyPairUtils.getGuidEntry(DefaultGNSClient.getDefaultGNSName(), localName);
				
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			      if (myGuid == null)
			      {
			        log.trace("No keys found for service " + localName + ". Generating new GUID and keys");
			        // Create a new GUID
			        myGuid = DefaultGNSClient.getGnsClient().guidCreate(DefaultGNSClient.getMyGuidEntry(), localName);
			        
			        // storing alias in gns record, need it to find it when we have GUID
			        // from group members
			        DefaultGNSClient.getGnsClient().fieldCreateList(myGuid.getGuid(), GnsConstants.ALIAS_FIELD, new JSONArray().put(localName), myGuid);
			        
			        DefaultGNSClient.getGnsClient()
			        	.fieldReplaceOrCreateList(myGuid.getGuid(), GNSKey, new JSONArray().put(GNSValue), myGuid);
			      }
			      else
			      {
			    	  System.out.println("GUID already registered, just replacing the field value "
			            + localName + " values " + GNSValue);
			    	  
			    	  long start = System.currentTimeMillis();
			        
			    	  DefaultGNSClient.getGnsClient()
		        	.fieldReplaceOrCreateList(myGuid.getGuid(), GNSKey, new JSONArray().put(GNSValue), myGuid);
			        long end = System.currentTimeMillis();
			        
			        System.out.println("writeKeyValue: fieldReplaceOrCreateList "+(end-start)+" curr Time "+System.currentTimeMillis());
			        
			        // JSONArray arr= gnsClient.fieldRead(myGuid.getGuid(), GNSKey, myGuid);
			        //System.out.println("read result "+gnsClient.lookupGuidRecord(myGuid.getGuid()));
			      }
			      
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
	}
	  
	  public static String getKeyValue(String guid, String field) throws Exception
	  {
	        JSONArray queryResult = DefaultGNSClient.getGnsClient().fieldReadArray(guid, field, null);
	        if(queryResult.length() > 0)
	        {
	        	return queryResult.getString(0);
	        } else
	        {
	        	return "";
	        }
	  }
	  
	  public static String getAlias(String guid) throws Exception
	  {
		  JSONObject record = DefaultGNSClient.getGnsClient().lookupGuidRecord(guid);
	      return record.getString("name");
	  }
	  
	  public static String getSHA1(String stringToHash) 
	  {
		   //Hashing.consistentHash(input, buckets);
		   MessageDigest md=null;
		   try 
		   {
			   md = MessageDigest.getInstance("SHA-256");
		   } 
		   catch (NoSuchAlgorithmException e) 
		   {
			   e.printStackTrace();
		   }
		   
		   md.update(stringToHash.getBytes());
		   
		   byte byteData[] = md.digest();
		 
		   //convert the byte to hex format method 1
		   StringBuffer sb = new StringBuffer();
		   for (int i = 0; i < byteData.length; i++) 
		   {
		   		sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
		   }
		   return sb.toString();
	  }
	  
	  /**
	   * adds the given address in the notification set
	   * @param socketAddress
	   * @param groupQuery
	   */
		public static void updateNotificationSetOfAGroup(InetSocketAddress socketAddress, String groupQuery, String groupGUID)
		{
			try
			{
			    /*
			     * Take a lock on the GNS connection object to prevent concurrent queries to
			     * the GNS on the same connection as the library is not thread-safe from
			     * that standpoint.
			     */
			      //if(groupGuid!=null)
			      {
			    	  //String groupGUIDString = gnsClient.lookupGuid(queryHash);
			    	  String addrString = socketAddress.getAddress().getHostAddress()+":"+socketAddress.getPort();
			    	  JSONArray arr = new JSONArray();
			    	  arr.put(addrString);
			    	  boolean alreadyThere = false;
			    	  
			    	  // separate try because if notification set field is not there then it will throw 
			    	  // exception. Exception shoudl be caught and notification set updated.
			    	  try
			    	  {
				    	  JSONArray notSet = DefaultGNSClient.getGnsClient().fieldReadArray(groupGUID,NOTIFICATION_SET, DefaultGNSClient.getMyGuidEntry());
				    	  System.out.println("\n\n notSet size "+notSet.length());
				    	  for(int i=0;i < notSet.length();i++)
				    	  {
				    		  System.out.println(" addStr "+addrString+" from set "+notSet.getString(i));
				    		  if( addrString.equals(notSet.getString(i)) )
				    		  {
				    			  alreadyThere = true;
				    			  break;
				    		  }
				    	  }
			    	  } catch(Exception ex)
			    	  {
			    		  log.trace("exception due to filed not there. not important");
			    		  //ex.printStackTrace();
			    	  }
			    	  if(!alreadyThere)
			    	  {
			    		  System.out.println("addrString "+addrString+" added to the notification set");
			    		  DefaultGNSClient.getGnsClient().fieldAppend(groupGUID, NOTIFICATION_SET, arr, DefaultGNSClient.getMyGuidEntry());
			    	  }
			      } 
			} catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
		
		
	 /**
	   * returns the GUIDs
	   * @param coordJson
	   * @param radius
	   * @return
	   * @throws Exception
	   */
	  public static JSONArray selectNear(JSONArray coordJson, double radius) throws Exception
	  {
		  JSONArray queryResult = DefaultGNSClient.getGnsClient().selectNear(GnsProtocol.LOCATION_FIELD_NAME, coordJson, radius);
		  log.trace("size of selected GUIDs "+ queryResult.length() );
		        
        // returns the list of GUIDs, read whole records and return 
        /*JSONArray queryRecords = new JSONArray();
			        
			        for (int i = 0; i < queryResult.length(); i++) 
				    {
				    	JSONObject obj = gnsClient.lookupGuidRecord(queryResult.getString(i));
				    	queryRecords.put(obj);
				    	
//				    	try {
//					        JSONObject record = queryResult.getJSONObject(i);
//					        
//					        Iterator<?> keys = record.keys();
	//
//					        //while( keys.hasNext() )
	 * 						{
//					        //    String key = (String)keys.next();
//					        //    
//					        //    System.out.println("Keys of JSON Object "+key);
//					        //    //if( jObject.get(key) instanceof JSONObject ){
//							//
//					        //    //}
//					        //}
//					        String guidString = record.getString("GUID");
//					        String attrGNSValue = record.getString(field);
//					        String compvalue = "[\""+ value+"\"]";
//					        System.out.println("guidString "+guidString+" attrGNSValue "+attrGNSValue);
//					        if( attrGNSValue.equals(compvalue) ) 
	 * 						{
//					        	currMem.add(guidString);
//					        }
//				    	} catch(Exception ex) 
//				    	{
//				    		log.trace("field not found exception in JSON Object" + ex.toString());
//				    		ex.printStackTrace();
//				    	}
				    }*/
        return queryResult;
	  }
}