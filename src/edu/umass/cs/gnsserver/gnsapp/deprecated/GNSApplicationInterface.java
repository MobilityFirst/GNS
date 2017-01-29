
package edu.umass.cs.gnsserver.gnsapp.deprecated;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gnsserver.activecode.ActiveCodeHandler;
import edu.umass.cs.gnsserver.gnsapp.clientCommandProcessor.ClientRequestHandlerInterface;
import edu.umass.cs.gnsserver.gnsapp.recordmap.BasicRecordMap;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableNodeConfig;
import org.json.JSONObject;

import java.io.IOException;

// Not really sure why this is in a deprecated package. Some parts of it are obsolete and
// need to be updated and removed, but the overall interface is necessary.

public interface GNSApplicationInterface<NodeIDType> {


  NodeIDType getNodeID();


  BasicRecordMap getDB();


  ReconfigurableNodeConfig<NodeIDType> getGNSNodeConfig();


  void sendToClient(Request response, JSONObject msg) throws IOException;


  void sendToID(NodeIDType id, JSONObject msg) throws IOException;


  ClientRequestHandlerInterface getRequestHandler();


  ActiveCodeHandler getActiveCodeHandler();

}
