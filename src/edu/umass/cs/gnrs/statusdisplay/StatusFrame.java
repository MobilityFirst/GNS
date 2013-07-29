/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.statusdisplay;

import edu.umass.cs.gnrs.main.GNS;
import edu.umass.cs.gnrs.packet.Packet;
import edu.umass.cs.gnrs.packet.TrafficStatusPacket;
import edu.umass.cs.gnrs.util.MoreUtils;
import edu.umass.cs.gnrs.util.ThreadUtils;
import javax.swing.JComponent;

/**
 *
 * @author westy
 */
public class StatusFrame extends javax.swing.JFrame implements UpdateListener {

  private static StatusFrame instance = null;

  public static StatusFrame getInstance() {
    if (instance == null) {
      instance = new StatusFrame();
    }
    return instance;
  }

  @Override
  public synchronized void update() {
    if (instance != null) {
      instance.panel.updateComponents();
      instance.pack();
    } 
  }

  /**
   * Creates new form StatusFrame
   */
  private StatusFrame() {
    initComponents();
  }
  StatusPanel panel;

  public StatusPanel getPanel() {
    return panel;
  }

  /**
   * This method is called from within the constructor to initialize the form. 
   */
  private void initComponents() {

    setDefaultCloseOperation(javax.swing.WindowConstants.EXIT_ON_CLOSE);

    panel = new StatusPanel();
    JComponent newContentPane = panel;
    newContentPane.setOpaque(true); //content panes must be opaque
    setContentPane(newContentPane);
    pack();
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String args[]) {
//    java.awt.EventQueue.invokeLater(new Runnable() {
//      @Override
//      public void run() {
//        StatusFrame frame = getInstance();
//        frame.setVisible(true);
//        StatusModel.getInstance().addUpdateListener(frame);
//      }
//    });
    test();
  }
  
  private static void test() {
    StatusFrame.getInstance().setVisible(true);
    StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());
    ThreadUtils.sleep(4000);
    for (int i = 1; i < 51; i++) {
      StatusModel.getInstance().queueAddEntry(i);
    }
    ThreadUtils.sleep(1000);
    StatusModel.getInstance().queueUpdate(1, StatusEntry.State.INITIALIZING);
    ThreadUtils.sleep(1000);
    StatusModel.getInstance().queueUpdate(2, StatusEntry.State.INITIALIZING);
    ThreadUtils.sleep(500);
    StatusModel.getInstance().queueUpdate(3, StatusEntry.State.INITIALIZING);
    ThreadUtils.sleep(500);
    StatusModel.getInstance().queueUpdate(4, StatusEntry.State.INITIALIZING);
    StatusModel.getInstance().queueUpdate(5, StatusEntry.State.INITIALIZING);
    ThreadUtils.sleep(500);
    StatusModel.getInstance().queueUpdate(1, "boo ya");
    ThreadUtils.sleep(1000);
    StatusModel.getInstance().queueUpdate(1, StatusEntry.State.RUNNING);
    
     StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(1, 5, GNS.PortType.DNS_PORT, Packet.PacketType.DNS_RESPONSE,  null, null));
     StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(4, 3, GNS.PortType.DNS_PORT, Packet.PacketType.DNS_ERROR_RESPONSE,  "E592EDC0DAD5E4DF8E5E79F22BAB6680D1899567", null));
          
//    StatusModel.getInstance().addSendNotation(1, 2, GNRS.PortType.DNS_PORT, Packet.PacketType.DNS, new Date());
//    StatusModel.getInstance().addSendNotation(2, 3, GNRS.PortType.LNS_UPDATE_PORT, Packet.PacketType.UPDATE_ADDRESS_LNS, new Date());
  }
}
