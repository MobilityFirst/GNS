package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.nio.InterfaceJSONNIOTransport;
import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
import edu.umass.cs.gns.nsdesign.Reconfigurable;
import edu.umass.cs.gns.nsdesign.Replicable;
import edu.umass.cs.gns.nsdesign.recordmap.BasicRecordMap;
import edu.umass.cs.gns.ping.PingManager;

/**
 * Interface for the GNS app. We have only one working implementation of this interface
 * {@link edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable}.
 * But, defining this interface helps us write a {@link edu.umass.cs.gns.nsdesign.gnsReconfigurable.DummyGnsReconfigurable}
 * which is useful during testing.
 *
 * Created by abhigyan on 5/19/14.
 */
public interface GnsReconfigurableInterface extends Replicable, Reconfigurable{
  int getNodeID();

  BasicRecordMap getDB();

  GNSNodeConfig getGNSNodeConfig();

  InterfaceJSONNIOTransport getNioServer();

  void reset() throws FailedDBOperationException;

  PingManager getPingManager();
}
