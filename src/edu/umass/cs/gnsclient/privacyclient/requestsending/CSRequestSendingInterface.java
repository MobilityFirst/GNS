package edu.umass.cs.gnsclient.privacyclient.requestsending;

import java.security.PublicKey;
import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.privacyclient.anonymizedID.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.privacyclient.common.ACLEntry;

/**
 * Interface defines sending of privacy 
 * preserving requests to CS via GNS.
 * @author adipc
 */
public interface CSRequestSendingInterface 
{
	public boolean sendPrivacyClientMessage(String targetGUID, JSONObject attrValuePair, 
    	HashMap<String, ACLEntry> aclMap, List<AnonymizedIDEntry> anonymizedIdList, 
    	String csGUID, PublicKey csPublicKey );
}