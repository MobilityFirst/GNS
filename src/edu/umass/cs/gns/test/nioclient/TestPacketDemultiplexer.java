package edu.umass.cs.gns.test.nioclient;

import edu.umass.cs.gns.localnameserver.IntercessorInterface;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nsdesign.packet.*;
import edu.umass.cs.gns.util.NSResponseCode;
import edu.umass.cs.gns.util.ResultValue;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Class created for testing {@link edu.umass.cs.gns.test.nioclient.DBClientIntercessor} but can be used to test
 * other intercessors as well.
 *
 * Our tests send requests to a local name server that is expected to send responses to requests. This class acts
 * a fake local name server that sends a responses to intercessor after some delay.
 *
 * This class currently understands only a subset of packet types that the actual local name server understands.
 *
 * Created by abhigyan on 6/20/14.
 */
public class TestPacketDemultiplexer extends AbstractPacketDemultiplexer{

  private Timer t = new Timer();
  private IntercessorInterface intercessor;

  public TestPacketDemultiplexer() { }

  public void setIntercessor(IntercessorInterface intercessor) {
    this.intercessor = intercessor;
  }

  @Override
  public boolean handleJSONObject(JSONObject json) {

    GNS.getLogger().fine("Fake LNS received request ... " + json);
    boolean isPacketTypeFound = true;
    JSONObject response = null;
    try {
      switch (Packet.getPacketType(json)) {
        case ADD_RECORD:
          response = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, new AddRecordPacket(json)).toJSONObject();
          break;
        case REMOVE_RECORD:
          response = new ConfirmUpdatePacket(NSResponseCode.NO_ERROR, new RemoveRecordPacket(json)).toJSONObject();
          break;
        case UPDATE:
          response = ConfirmUpdatePacket.createSuccessPacket(new UpdatePacket(json)).toJSONObject();
          break;
        case DNS:
          DNSPacket dnsPacket = new DNSPacket(json);
          dnsPacket.getHeader().setResponseCode(NSResponseCode.NO_ERROR);
          dnsPacket.getHeader().setQRCode(DNSRecordType.RESPONSE);
          ResultValue value = new ResultValue();
          value.add("ABCD");
          dnsPacket.setSingleReturnValue(value);
          response = dnsPacket.toJSONObject();
          break;
        default:
          isPacketTypeFound = false;
      }
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (response != null) sendWithDelay(response);
    return isPacketTypeFound;
  }

  private void sendWithDelay(final JSONObject json) {
    t.schedule(new TimerTask() {
      @Override
      public void run() {
        GNS.getLogger().fine("Fake LNS sent response ... " + json);
        intercessor.handleIncomingPacket(json);
      }
    }, new Random().nextInt(1000));
  }

}
