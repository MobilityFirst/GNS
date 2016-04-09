package edu.umass.cs.gnsclient.privacyclient.messages;

import org.json.JSONObject;

/**
 * This class describes the privacy client message format.
 * GNS Client messages are encrypted with a 
 * symmetric key which is encrypted with public key of GNS.
 * Recipient of this message information is also included.
 * Recipient of this message can be GNS or CS.
 * 
 * If GNS is recipient then GNS nameservers/Active replica 
 * should decrypt the message and process it.
 * 
 * If CS is recipient then GNS nameservers/Active replica should forward it to CS.
 * 
 * @author adipc
 */
public class PrivacyClientMessage
{
	// requestID of message.
	private final long requestID;
	
	// GUID of recipient. It can be GNS's or CS's GUID
	private final String recipientGUID;
	
	// GNS command or the CS message is encrypted with a symmetric key.
	// then the symeetric key is encrypted with the recipients public key.
	// as this message can be big so encrypting it directly with the 
	// recipient public key will be high overhead.
	private final byte[] encryptedGNSCommandJSON;
	
	// symmetric key encrypted with the recipient's public key.
	private final byte[] encryptedSymmetricKey;
	
	public PrivacyClientMessage(long requestID, String recipientGUID, 
			byte[] encryptedGNSCommandJSON, byte[] encryptedSymmetricKey)
	{
		this.requestID = requestID;
		this.recipientGUID = recipientGUID;
		this.encryptedGNSCommandJSON = encryptedGNSCommandJSON;
		this.encryptedSymmetricKey = encryptedSymmetricKey;
	}
	
	
	public JSONObject toJSONObject()
	{
		return new JSONObject();
	}
}