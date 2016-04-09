package edu.umass.cs.gnsclient.privacyclient;


import java.io.IOException;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsclient.client.CommandUtils;
import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.client.GuidEntry;
import edu.umass.cs.gnsclient.privacyclient.anonymizedID.AnonymizedIDCreationInterface;
import edu.umass.cs.gnsclient.privacyclient.anonymizedID.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.privacyclient.anonymizedID.NoopAnonymizedIDCreator;
import edu.umass.cs.gnsclient.privacyclient.common.ACLEntry;
import edu.umass.cs.gnsclient.privacyclient.messages.PrivacyClientMessage;
import edu.umass.cs.gnsclient.privacyclient.requestsending.CSRequestSending;
import edu.umass.cs.gnsclient.privacyclient.requestsending.CSRequestSendingInterface;
import edu.umass.cs.gnsclient.privacyclient.requestsending.GNSRequestSending;
import edu.umass.cs.gnsclient.privacyclient.requestsending.GNSRequestSendingInterface;
import edu.umass.cs.gnsclient.privacyclient.valueencryption.ValueEncryption;
import edu.umass.cs.gnsclient.privacyclient.valueencryption.ValueEncryptionInterface;
import edu.umass.cs.gnscommon.GnsProtocol;
import edu.umass.cs.gnscommon.exceptions.client.GnsClientException;

/**
 * This client implements all the GNS client read and 
 * update methods in a privacy preserving manner.
 * The update methods only include updates to the atrribute-value
 * pairs. ACL updates should be done through GNSClient.
 * This client also consists of methids to create anonymized IDs.
 * @author adipc
 *
 */
public class PrivacyPreservingGNSClient 
{
	private final GNSClient gnsClient;
	
	// needed to fetch subspace configuration information.
	private final ContextServiceClient<Integer> csClient;
	
	// for anonymization of GUIDs in CS.
	private final AnonymizedIDCreationInterface anonymizedIDCreator;
	// for encryption of values in GNS.
	private final ValueEncryptionInterface valueEncryption;
	
	
	// to send a request to CS via GNS.
	// one gns update might lead to updates to multiple anonymized IDs.
	// so all that logic is implemented in the class implementing this interface.
	private final CSRequestSendingInterface csRequestSending;
	
	
	// for sending the request to GNS and check that request
	// executed successfully.
	private final GNSRequestSendingInterface gnsRequestSending;
	
	
	// GNS public key and guid.
	private PublicKey gnsPublicKey;
	private String gnsGUID;
	
	private PublicKey csPublicKey;
	private String csGUID;
	
	
	
	public PrivacyPreservingGNSClient() throws IOException
	{
		//FIXME: not a right instantiation of gnsClient.
		gnsClient = new GNSClient();
		csClient = new ContextServiceClient<Integer>("127.0.0.1", 8000);
		
		valueEncryption = new ValueEncryption();
		anonymizedIDCreator = new NoopAnonymizedIDCreator();
		
		csRequestSending = new CSRequestSending(gnsClient);
		gnsRequestSending = new GNSRequestSending(gnsClient);
	}
	
	public List<AnonymizedIDEntry> computeAnonymizedIDs
										(HashMap<String, List<ACLEntry>> aclList)
	{
		return anonymizedIDCreator.computeAnonymizedIDs(aclList);
	}
	
	// Each method of update and read is implemented here in privacy preserving manner.
	
	
	/**
	   * Replaces the values of the field in targetGuid with the single value or creates a new
	   * field with a single value list if it does not exist.
	   * If the writer is different use addToACL first to allow other
	   * the guid to write this field.
	   *
	   * @param targetGuid
	   * @param field
	   * @param value
	   * @param writer
	   * @param aclMap
	   * @throws IOException
	   * @throws GnsClientException
	   */
	  public void fieldReplaceOrCreateWithPrivacy(String targetGuid, String field, String value, 
			  GuidEntry writer, HashMap<String, ACLEntry> aclMap, List<AnonymizedIDEntry> anonymizedIdList)
	          throws IOException, GnsClientException 
	  {
		  // encryptrion of value
		String encryptedValueJSONString 
			=  valueEncryption.encryptValue(field, value, aclMap.get(field));
		
	    JSONObject command = CommandUtils.createAndSignCommand(writer.getPrivateKey(), GnsProtocol.REPLACE_OR_CREATE, GnsProtocol.GUID, targetGuid,
	            GnsProtocol.FIELD, field, GnsProtocol.VALUE, encryptedValueJSONString, GnsProtocol.WRITER, writer.getGuid());
	    
	    gnsRequestSending.sendPrivacyClientMessage(command, gnsGUID, gnsPublicKey);
	    
	    JSONObject attrValuePair = new JSONObject();
	    
	    csRequestSending.sendPrivacyClientMessage
	    (targetGuid, attrValuePair, aclMap, anonymizedIdList, targetGuid, csPublicKey);
	  }
}