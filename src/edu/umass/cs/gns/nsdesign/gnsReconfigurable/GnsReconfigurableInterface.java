package edu.umass.cs.gns.nsdesign.gnsReconfigurable;

import edu.umass.cs.gns.exceptions.FailedDBOperationException;
import edu.umass.cs.gns.gigapaxos.deprecated.Replicable;
import edu.umass.cs.gns.newApp.GnsApplicationInterface;
import edu.umass.cs.gns.nsdesign.Reconfigurable;
import edu.umass.cs.gns.util.Shutdownable;

/**
 * Interface for the GNS app. We have only one working implementation of this interface
 * {@link edu.umass.cs.gns.nsdesign.gnsReconfigurable.GnsReconfigurable}.
 * But, defining this interface helps us write a {@link edu.umass.cs.gns.nsdesign.gnsReconfigurable.DummyGnsReconfigurable}
 * which is useful during testing.
 *
 * Created by abhigyan on 5/19/14.
 * @param <NodeIDType>
 */
@Deprecated
public interface GnsReconfigurableInterface<NodeIDType> extends GnsApplicationInterface, Replicable, Reconfigurable, Shutdownable{

  void reset() throws FailedDBOperationException;

}
