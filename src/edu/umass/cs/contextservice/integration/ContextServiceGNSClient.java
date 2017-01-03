package edu.umass.cs.contextservice.integration;


import org.json.JSONObject;

import edu.umass.cs.contextservice.client.ContextServiceClient;
import edu.umass.cs.contextservice.client.callback.implementations.NoopCallBack;
import edu.umass.cs.contextservice.client.callback.implementations.NoopUpdateReply;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import edu.umass.cs.gnsserver.main.GNSConfig;

import java.util.logging.Level;
import edu.umass.cs.gnscommon.GNSProtocol;

/**
 * This class interacts with context service using context service client.
 * It also implements the ContextServiceInterface.
 *
 * @author adipc
 *
 */
public class ContextServiceGNSClient implements ContextServiceGNSInterface {

  private ContextServiceClient<Integer> csClient;
  private NoopCallBack csNoopCallBack;
  private NoopUpdateReply csNoopUpdateReply;

  /**
   *
   * @param hostName
   * @param portNum
   */
  public ContextServiceGNSClient(String hostName, int portNum) 
  {
    // catching everything here, otherwise exception doesn't get printed in executor service.
    try 
    {
      csClient = new ContextServiceClient<Integer>(hostName, portNum);
    } catch (Error | Exception er) {
      er.printStackTrace();
    }
    
    csNoopCallBack = new NoopCallBack();
    csNoopUpdateReply = new NoopUpdateReply();
  }
  

//	@Override
//	public void sendUpdateToCS(String GNSProtocol.GUID.toString(), JSONObject attrValPairJSON, long versionNum, boolean blocking) 
//	{	
//	}
  @Override
  public void sendTiggerOnGnsCommand(JSONObject jsonFormattedCommand, AbstractCommand command, boolean blocking) {
    try {
      // code copied exactly from AbstractUpdate class
      String guid = jsonFormattedCommand.getString(GNSProtocol.GUID.toString());
      String field = jsonFormattedCommand.optString(GNSProtocol.FIELD.toString(), null);
      String value = jsonFormattedCommand.optString(GNSProtocol.VALUE.toString(), null);
      String oldValue = jsonFormattedCommand.optString(GNSProtocol.OLD_VALUE.toString(), null);
      int index = jsonFormattedCommand.optInt(GNSProtocol.N.toString(), -1);
      JSONObject userJSON = jsonFormattedCommand.has(GNSProtocol.USER_JSON.toString()) ? new JSONObject(jsonFormattedCommand.getString(GNSProtocol.USER_JSON.toString())) : null;
      // writer might be unspecified so we use the guid
      String writer = jsonFormattedCommand.optString(GNSProtocol.WRITER.toString(), guid);
      String signature = jsonFormattedCommand.optString(GNSProtocol.SIGNATURE.toString(), null);
      String message = jsonFormattedCommand.optString(GNSProtocol.SIGNATUREFULLMESSAGE.toString(), null);

      if (writer.equals(
    		  GNSProtocol.INTERNAL_QUERIER.toString()
    		  //GNSConfig.getInternalOpSecret()
    		  //Config.getGlobalString(GNSConfig.GNSC.INTERNAL_OP_SECRET)
    		  )) {
        writer = null;
      }

      if (field == null) {
        //responseCode = FieldAccess.updateUserJSON(guid, userJSON, writer, signature, message, handler);

        // full json update
        // send the full JSON, contextServiceClient will check what attributes are supported 
        // by context service and send them to CS
        GNSConfig.getLogger().log(Level.FINE, "Trigger to CS guid {0} userJSON {1}",
                new Object[]{guid, userJSON});

        csClient.sendUpdateWithCallBack(guid, null, userJSON, -1, 
        		csNoopUpdateReply, csNoopCallBack);
      } else {
        // single field update
        JSONObject attrValJSON = new JSONObject();
        attrValJSON.put(field, value);

        GNSConfig.getLogger().log(Level.FINE, "Trigger to CS guid {0} attrValJSON {1}",
                new Object[]{guid, attrValJSON});

        csClient.sendUpdateWithCallBack(guid, null, attrValJSON, -1, 
        		csNoopUpdateReply, csNoopCallBack);
      }
    } catch (Exception | Error ex) {
      ex.printStackTrace();
    }
  }

}
