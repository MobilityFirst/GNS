package edu.umass.cs.gnsclient.privacyclient.requestsending;

import java.security.PublicKey;

import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GNSClient;

/**
 * GNS request sending class. Implements the code to send a 
 * privacy preserving request to GNS.
 * @author adipc
 *
 */
public class GNSRequestSending implements GNSRequestSendingInterface
{
	private final GNSClient gnsClient;
	
	public GNSRequestSending(GNSClient gnsClient)
	{
		this.gnsClient = gnsClient;
	}

	@Override
	public boolean sendPrivacyClientMessage(JSONObject gnsCommand, 
			String gnsGUID, PublicKey gnsPublicKey) 
	{
//		PrivacyClientMessage privacyMessage 
//		= createPrivacyClientMessage(gnsCommand, 
//		    		gnsGUID, gnsPublicKey );
		// String response = sendCommandAndWait( command );
		// checkResponse( command, response );
		
		return false;
	}
}