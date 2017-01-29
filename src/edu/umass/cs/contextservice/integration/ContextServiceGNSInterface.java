package edu.umass.cs.contextservice.integration;

import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.commands.AbstractCommand;
import org.json.JSONObject;


public interface ContextServiceGNSInterface {


  //public void sendUpdateToCS(String GUID, JSONObject attrValPairJSON, long versionNum, boolean blocking);


  public void sendTiggerOnGnsCommand(JSONObject jsonFormattedCommand, AbstractCommand command, boolean blocking);
}
