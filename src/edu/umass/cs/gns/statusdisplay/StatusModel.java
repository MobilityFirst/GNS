package edu.umass.cs.gns.statusdisplay;

import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.main.GNS.PortType;
import edu.umass.cs.gns.packet.Packet;
import edu.umass.cs.gns.packet.admin.TrafficStatusPacket;
import edu.umass.cs.gns.statusdisplay.StatusEntry.State;
import edu.umass.cs.gns.util.Format;
import java.awt.geom.Point2D;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.swing.SwingUtilities;

/**
 *
 * @author westy
 */
public class StatusModel {

  private final static StatusModel instance = new StatusModel();

  public static StatusModel getInstance() {
    return instance;
  }
  private Map<Integer, StatusEntry> entries = new ConcurrentHashMap<Integer, StatusEntry>();
  //private CopyOnWriteArrayList<String> entryUpdates = new CopyOnWriteArrayList<String>();
  private Set<UpdateListener> listeners = new HashSet<UpdateListener>();

  public synchronized void addUpdateListener(UpdateListener listener) {
    listeners.add(listener);
  }

  public synchronized void removeUpdateListener(UpdateListener listener) {
    listeners.remove(listener);
  }

  public Collection<StatusEntry> getEntries() {
    return entries.values();
  }

  public StatusEntry getEntry(int id) {
    return entries.get(id);
  }

  public static class SendNotation {

    private int sender;
    private int receiver;
    private GNS.PortType portType;
    private Packet.PacketType packetType;
    private Date time;
    private String name;
    private String key;
    private String other;

    public int getSender() {
      return sender;
    }

    public int getReceiver() {
      return receiver;
    }

    public PortType getPortType() {
      return portType;
    }

    public Date getTime() {
      return time;
    }

    public Packet.PacketType getPacketType() {
      return packetType;
    }

    public String getName() {
      return name;
    }

    public String getKey() {
      return key;
    }

    public String getOther() {
      return other;
    }

    public SendNotation(TrafficStatusPacket pkt) {
      this.sender = pkt.getFromID();
      this.receiver = pkt.getToID();
      this.portType = pkt.getPortType();
      this.packetType = pkt.getPacketType();
      this.time = pkt.getTime();
      this.name = pkt.getName();
      //this.key = pkt.getKey();
      this.other = pkt.getOther();
    }

    @Override
    public String toString() {
      return "SendNotation{" + "sender=" + sender + ", receiver=" + receiver + ", portType=" + portType + ", packetType=" + packetType + ", time=" + time + ", name=" + name + ", key=" + key + ", other=" + other + '}';
    }
  }
  private List<SendNotation> sendNotations = new LinkedList<SendNotation>();

  public List<SendNotation> getSendNotations() {
    synchronized (sendNotations) {
      // send in the clones
      return new ArrayList<SendNotation>(sendNotations);
    }
  }

  @Override
  public String toString() {
    return "StatusModel{" + "entries=" + entries + ", sendNotations=" + sendNotations + '}';
  }

  // the update queuing part
  public class EntryUpdate {

    Integer newid; // so it can be null
    int id;
    Date time;
    StatusEntry.State newState;
    String statusString;
    String name;
    String ip;
    Point2D location;
    boolean clear;

    public EntryUpdate(boolean isNew, int newid) {
      this.newid = newid;
    }

    public EntryUpdate(boolean clear) {
      this.clear = true;
    }

    public EntryUpdate(int id) {
      this.newid = null;
      this.clear = false;
      this.id = id;
      this.time = null;
      this.newState = null;
      this.statusString = null;
      this.name = null;
      this.ip = null;
      this.location = null;
    }

    public EntryUpdate(int id, String name, String ip, Point2D location) {
      this(id);
      this.name = name;
      this.ip = ip;
      this.location = location;
    }

    public EntryUpdate(int id, String statusString, Date time) {
      this(id);
      this.statusString = statusString;
      this.time = time;
    }

    public EntryUpdate(int id, State newState) {
      this(id);
      this.newState = newState;
    }

    public EntryUpdate(int id, String statusString) {
      this.id = id;
      this.statusString = statusString;
    }

    public EntryUpdate(int id, State newState, String statusString) {
      this.id = id;
      this.newState = newState;
      this.statusString = statusString;
    }

    public EntryUpdate(int id, Date time) {
      this.id = id;
      this.time = time;
    }

    public int getId() {
      return id;
    }

    public Integer getNewid() {
      return newid;
    }

    public Date getTime() {
      return time;
    }

    public State getNewState() {
      return newState;
    }

    public String getStatusString() {
      return statusString;
    }

    public String getName() {
      return name;
    }

    public String getIp() {
      return ip;
    }

    public Point2D getLocation() {
      return location;
    }

    public boolean isClear() {
      return clear;
    }
  }

  /**
   * contains either a traffic status update or a status entry update, but not both *
   */
  public class ModelEvent {

    TrafficStatusPacket packet;
    EntryUpdate entryUpdate;

    public ModelEvent(TrafficStatusPacket packet) {
      this.packet = packet;
      this.entryUpdate = null;
    }

    public ModelEvent(EntryUpdate entryUpdate) {
      this.entryUpdate = entryUpdate;
      this.packet = null;
    }

    public TrafficStatusPacket getPacket() {
      return packet;
    }

    public EntryUpdate getEntryUpdate() {
      return entryUpdate;
    }
  }

  public void queueAddEntry(int id) {
    addModelEvent(new ModelEvent(new EntryUpdate(true, id)));
  }

  public void queueDeleteAllEntries() {
    addModelEvent(new ModelEvent(new EntryUpdate(false)));
  }

  public void queueUpdate(int id, String statusString, Date time) {
    addModelEvent(new ModelEvent(new EntryUpdate(id, statusString, time)));
  }

  public void queueUpdate(int id, String name, String ip, Point2D location) {
    addModelEvent(new ModelEvent(new EntryUpdate(id, name, ip, location)));
  }

  public void queueUpdate(int id, StatusEntry.State newState) {
    addModelEvent(new ModelEvent(new EntryUpdate(id, newState)));
  }

  public void queueUpdate(int id, String statusString) {
    addModelEvent(new ModelEvent(new EntryUpdate(id, statusString)));
  }

  public void queueUpdate(int id, StatusEntry.State newState, String statusString) {
    addModelEvent(new ModelEvent(new EntryUpdate(id, newState, statusString)));
  }

  public void queueUpdate(int id, Date time) {
    addModelEvent(new ModelEvent(new EntryUpdate(id, time)));
  }

  public void queueSendNotation(TrafficStatusPacket pkt) {
    addModelEvent(new ModelEvent(pkt));
  }
  //
  private final BlockingQueue<ModelEvent> modelEvents = new LinkedBlockingQueue<ModelEvent>();
  //
  private static final int MAXNOTATIONSSIZE = 10000;
  private final Runnable processEventsRunnable = new Runnable() {
    @Override
    public void run() {
      ModelEvent evt;
      while ((evt = modelEvents.poll()) != null) {
        // these events contain either a traffic status update or a status entry update, but not both
        if (evt.getPacket() != null) {
          sendNotations.add(new SendNotation(evt.getPacket()));
          // limit the size of this list
          if (sendNotations.size() > MAXNOTATIONSSIZE) {
            sendNotations = sendNotations.subList(sendNotations.size() - MAXNOTATIONSSIZE, sendNotations.size());
          }
        } else {
          EntryUpdate update = evt.getEntryUpdate();
          if (update.isClear()) {
            entries.clear();
          } else if (update.getNewid() != null) {
            int id = update.getNewid();
            if (entries.get(id) != null) {
              System.out.println("Bad stuff: entry " + id + " already exists");
            } else {
              entries.put(id, new StatusEntry(id));
            }
          } else {
            StatusEntry entry = getEntry(update.getId());
            if (entry != null) { // just in case
              if (update.getTime() != null) {
                entry.setTime(update.getTime());
              } else {
                entry.setTime(new Date());
              }
              if (update.getStatusString() != null) {
                entry.setStatusString(update.getStatusString());
              }
              if (update.getNewState() != null) {
                entry.setState(update.getNewState());
              }
              if (update.getName() != null) {
                entry.setName(update.getName());
              }
              if (update.getIp() != null) {
                entry.setIp(update.getIp());
              }
              if (update.getLocation() != null) {
                entry.setLocation(update.getLocation());
              }
            }
          }
        }
      }
      for (UpdateListener listener : listeners) {
        listener.update();
      }
    }
  };

  // Called by thread other than event dispatch thread.  Adds event to
  // "pending" queue ready to be processed.
  public void addModelEvent(ModelEvent evt) {
    modelEvents.add(evt);

    // Optimisation 1: Only invoke EDT if the queue was previously empty before
    // adding this event.  If the size is 0 at this point then the EDT must have
    // already been active and removed the event from the queue, and if the size
    // is > 1 we know that the EDT must have already been invoked in a previous
    // method call but not yet drained the queue (i.e. so no need to invoke it
    // again).
    if (modelEvents.size() == 1) {
      // Optimisation 2: Do not create a new Runnable each time but use a stateless
      // inner class to drain the queue and update the table model.
      SwingUtilities.invokeLater(processEventsRunnable);
    }
  }
}
