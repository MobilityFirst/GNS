package edu.umass.cs.gnsserver.activecode.prototype.interfaces;

import org.json.JSONObject;

import edu.umass.cs.gnsserver.activecode.prototype.ActiveException;

/**
 * @author gaozy
 *
 */
public interface ACLQuerier {
	/**
	 * This method allows customer's active code to look up a user name of a guid.
	 * <p> This is useful when the code needs to decide whether the guid can access
	 * to the field, i.e., access control
	 * 
	 * @param guid
<<<<<<< HEAD
	 * @return a JSONObject object that contains the targetGuid's public key with the key {@ActiveCode.PUBLICKEY_FIELD}.
=======
	 * @return a JSONObject object that contains the targetGuid's public key 
         * with the key ActiveCode.PUBLICKEY_FIELD.
>>>>>>> upstream/master
	 * @throws ActiveException throws an ActiveException if the targetGuid does not exist.
	 */
	public JSONObject lookupUsernameForGuid(String guid) throws ActiveException;
}
