package edu.umass.cs.gnsclient.privacyclient.requestsending;

import java.security.PublicKey;

import org.json.JSONObject;

/**
 * Interface defines sending privacy preserving requests to GNS
 * @author adipc
 */
public interface GNSRequestSendingInterface 
{
	/**
	 * creates and sends privacy client message and sends it to GNS
	 * @param gnsCommand
	 * @param gnsGUID
	 * @param gnsPublicKey
	 * @return
	 */
	public boolean sendPrivacyClientMessage(JSONObject gnsCommand, 
    		String gnsGUID, PublicKey gnsPublicKey );
}