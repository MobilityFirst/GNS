package edu.umass.cs.gns.protocoltask;

import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import edu.umass.cs.gns.nio.GenericMessagingTask;

/**
 * @author V. Arun
 */
/*
 * The point of this class is to wrap the user's ProtocolTask interface compliant
 * task into one wherein we could store and manipulate internal variables, e.g.,
 * for killing after a long idle timeout. Alternatively, we could have made
 * ProtocolTask itself an abstract class instead of an interface, but the latter
 * is preferable.
 */
class ProtocolTaskWrapper<NodeIDType, EventType, KeyType>
        implements SchedulableProtocolTask<NodeIDType, EventType, KeyType> {

  protected static final long MAX_IDLE_TIME = 300000; // 5 mins
  protected static final long MAX_LIFETIME = 1800000; // 30 mins

  private final ProtocolTask<NodeIDType, EventType, KeyType> task;
  private long startTime = System.currentTimeMillis();
  private long lastActiveTime = System.currentTimeMillis();
  private ScheduledFuture<?> future;

  ProtocolTaskWrapper(ProtocolTask<NodeIDType, EventType, KeyType> task) {
    this.task = task;
  }

  @Override
  public KeyType getKey() {
    return this.task.getKey();
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] handleEvent(ProtocolEvent<EventType, KeyType> event,
          ProtocolTask<NodeIDType, EventType, KeyType>[] ptasks) {
    this.lastActiveTime = System.currentTimeMillis();
    return this.task.handleEvent(event, ptasks);
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] start() {
    this.startTime = System.currentTimeMillis();
    return this.task.start();
  }

  @Override
  public KeyType refreshKey() {
    return this.task.refreshKey();
  }

  protected void setFuture(final ScheduledFuture<?> future) {
    this.future = future;
  }

  protected ScheduledFuture<?> getFuture() {
    return this.future;
  }

  protected boolean isLongIdle() {
    return (System.currentTimeMillis() - this.lastActiveTime > MAX_IDLE_TIME) ? true
            : false;
  }

  protected boolean expired() {
    return (System.currentTimeMillis() - this.startTime > MAX_LIFETIME) ? true
            : false;
  }

  @Override
  public GenericMessagingTask<NodeIDType, ?>[] restart() {
    GenericMessagingTask<NodeIDType, ?>[] mtasks
            = (this.task instanceof SchedulableProtocolTask
                    ? ((SchedulableProtocolTask<NodeIDType, EventType, KeyType>) (this.task)).restart()
                    : this.task.start()); // if schedulable, call restart
    // if threshold task, filter out members already heard from
    if (this.task instanceof ThresholdProtocolTask) {
      ((ThresholdProtocolTask) (this.task)).fix(mtasks);
    }
    return mtasks;
  }

  @Override
  public Set<EventType> getEventTypes() {
    return this.task.getEventTypes();
  }
}
