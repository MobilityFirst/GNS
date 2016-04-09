package edu.umass.cs.gnsclient.privacyclient.valueencryption;

import org.json.JSONObject;

/**
 * This class denotes the JSONObject representation 
 * of the encrypted value. It contains the encrypted value 
 * and the associated ACL info to decrypt it.
 * @author adipc
 */
public class EncryptedValueJSON 
{
	private enum Keys {ENCRYPTED_VALUE, DECRYPT_VALUE_INFO};
	
	// byte arrays should be used instead if strings,
	// as string just double the space, 
	// and encrypted values are already very big.
	private final byte[] encytpedValue; 
	
	// the key of this JSONObject is guids in ACL, like G1 ,and the value is
	// and the value is enc(G1+, Ks), where G1+ is the public key of G1
	// and Ks is the symmetric key with which the value is encrypted.
	// having GUID G1 as the key helps in directly obtaining the decrypting
	// info rather than checking all members of ACL, decrypting and failing until
	// the owners own GUID is found in the ACL.
	// TODO: GNS can also jsut return the decrypting info of the GUID that is
	// doing the lookup, instead returning all members of ACL. but that
	// more changes in GNS for privacy stuff.
	private final JSONObject decryptValueInfo;
	
	
	public EncryptedValueJSON(byte[] encytpedValue, JSONObject decryptValueInfo)
	{
		this.encytpedValue = encytpedValue;
		this.decryptValueInfo = decryptValueInfo;
	}
	
	public JSONObject toJSONObject()
	{
		// TODO: completion of this 
		return new JSONObject();
		//JSONObject valueJSON = new JSONObject();
		//valueJSON.put(key, value);
	}
}