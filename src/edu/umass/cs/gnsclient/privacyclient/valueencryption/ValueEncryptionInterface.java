package edu.umass.cs.gnsclient.privacyclient.valueencryption;

import edu.umass.cs.gnsclient.privacyclient.common.ACLEntry;

/**
 * This interface defines function for value encryption.
 * In field and value in GNS. Value is encrypted so that 
 * it could be read only by ACL members. 
 * 
 * 
 * @author adipc
 *
 */
public interface ValueEncryptionInterface 
{
	/**
	 * Encrypts the plainTextValue and returns the String format of the JSONObject.
	 * @param field
	 * @param plainTextValue
	 * @param fieldACL
	 * @return
	 */
	public String encryptValue(String field, String plainTextValue, ACLEntry fieldACL);
}