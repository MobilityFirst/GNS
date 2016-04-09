package edu.umass.cs.gnsclient.privacyclient.requestsending;

import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;
import edu.umass.cs.gnsclient.privacyclient.anonymizedID.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.privacyclient.common.ACLEntry;

/**
 * Implements sending privacy client message to CS via GNS
 * @author adipc
 *
 */
public class CSRequestSending implements CSRequestSendingInterface
{
	private final GNSClient gnsClient;
	
	public CSRequestSending(GNSClient gnsClient)
	{
		this.gnsClient = gnsClient;
	}
	
	
	@Override
	public boolean sendPrivacyClientMessage(String targetGUID, JSONObject attrValuePair,
			HashMap<String, ACLEntry> aclMap, List<AnonymizedIDEntry> anonymizedIdList, String csGUID,
			PublicKey csPublicKey) 
	{
		// implements request to CS . 
		// like which anonymized IDs needs to be updated. 
		return false;
	}
}