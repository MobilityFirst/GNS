package edu.umass.cs.contextservice.integration;

import static edu.umass.cs.gnscommon.GnsProtocol.BAD_RESPONSE;
import static edu.umass.cs.gnscommon.GnsProtocol.FIELD;
import static edu.umass.cs.gnscommon.GnsProtocol.GUID;
import static edu.umass.cs.gnscommon.GnsProtocol.MAGIC_STRING;
import static edu.umass.cs.gnscommon.GnsProtocol.N;
import static edu.umass.cs.gnscommon.GnsProtocol.OK_RESPONSE;
import static edu.umass.cs.gnscommon.GnsProtocol.OLD_VALUE;
import static edu.umass.cs.gnscommon.GnsProtocol.SIGNATURE;
import static edu.umass.cs.gnscommon.GnsProtocol.SIGNATUREFULLMESSAGE;
import static edu.umass.cs.gnscommon.GnsProtocol.USER_JSON;
import static edu.umass.cs.gnscommon.GnsProtocol.VALUE;
import static edu.umass.cs.gnscommon.GnsProtocol.WRITER;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.gnsserver.gnsapp.NSResponseCode;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.CommandResponse;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commandSupport.FieldAccess;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.GnsCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnsserver.utils.ResultValue;

/**
 * This class interacts with context service 
 * using context service client. 
 * It also implements the ContextServiceInterface.
 * 
 * @author adipc
 *
 */
public class ContextServiceGNSClient implements ContextServiceGNSInterface
{
	private ContextServiceClient<Integer> csClient;
	public ContextServiceGNSClient(String hostName, int portNum)
	{
		// catching everything here, otherwise exception doesn't get printed in executor service.
		try
		{
			csClient = new ContextServiceClient<Integer>(hostName, portNum);
		}
		catch(Error er)
		{
			er.printStackTrace();
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
//	@Override
//	public void sendUpdateToCS(String GUID, JSONObject attrValPairJSON, long versionNum, boolean blocking) 
//	{	
//	}

	@Override
	public void sendTiggerOnGnsCommand(JSONObject jsonFormattedCommand, GnsCommand command, boolean blocking) 
	{
		try
		{
			// code copied exactly from AbstractUpdate class
			String guid = jsonFormattedCommand.getString(GUID);
			String field = jsonFormattedCommand.optString(FIELD, null);
			String value = jsonFormattedCommand.optString(VALUE, null);
			String oldValue = jsonFormattedCommand.optString(OLD_VALUE, null);
			int index = jsonFormattedCommand.optInt(N, -1);
			JSONObject userJSON = jsonFormattedCommand.has(USER_JSON) ? new JSONObject(jsonFormattedCommand.getString(USER_JSON)) : null;
			// writer might be unspecified so we use the guid
			String writer = jsonFormattedCommand.optString(WRITER, guid);
			String signature = jsonFormattedCommand.optString(SIGNATURE, null);
			String message = jsonFormattedCommand.optString(SIGNATUREFULLMESSAGE, null);
			
			if (writer.equals(MAGIC_STRING)) {
				writer = null;
			}
			
			if (field == null) {
			    //responseCode = FieldAccess.updateUserJSON(guid, userJSON, writer, signature, message, handler);
			    
			    // full json update
			    // send the full JSON, contextServiceClient will check what attributes are supported 
			    // by context service and send them to CS
				GNSConfig.getLogger().fine("Trigger to CS guid "+guid
	    				+" userJSON " +userJSON);
				
			    csClient.sendUpdate(guid, userJSON, -1, blocking);
			  } else {
			    // single field update
				JSONObject attrValJSON = new JSONObject();
				attrValJSON.put(field, value);
				
				GNSConfig.getLogger().fine("Trigger to CS guid "+guid
	    				+" attrValJSON " +attrValJSON);
				
				csClient.sendUpdate(guid, attrValJSON, -1, blocking);
			  }
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error er)
		{
			er.printStackTrace();
		}
}
	
}