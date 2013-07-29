/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.umass.cs.gnrs.statusdisplay;

import edu.umass.cs.gnrs.packet.Packet;
import edu.umass.cs.gnrs.util.Colors;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.geom.Line2D;
import javax.swing.JPanel;
import org.westy.jmapviewer.Draw;

/**
 *
 * @author westy
 */
public class KeyPanel extends JPanel {

  public static Font font = new Font("Arial", Font.PLAIN, 10);
  public static BasicStroke stroke = new BasicStroke((float) 2);

  public KeyPanel() {
    setBackground(Colors.White);
    setForeground(Colors.Black);
  }
  private static final int objectspacing = 50;

  @Override
  public void paintComponent(Graphics g) {
    super.paintComponent(g);
    Graphics2D g2 = (Graphics2D) g;
    Dimension size = getSize();
    double left = 10;
    double right = size.getWidth() - 10;
    double y = 10;
    double lineSpacing = 14; // 130 / PacketClass.values().length + 1;
    g2.setFont(font);
    g2.setStroke(stroke);
    for (Packet.PacketType p : MapFrame.packetGraphics.keySet()) {
      g2.setColor(Color.BLACK);
      Draw.drawStringBoxed(g2, p.toString(), 10, y);
      //g2.setColor(Colors.BlueViolet);
      g2.setColor(MapFrame.packetGraphics.get(p).getColor());
      g2.draw(new Line2D.Double(left, y, right, y));
      y = y + lineSpacing;
    }

//    g2.setColor(Colors.Black);
//    y = y + lineSpacing;
//    //IconUtils.paintAt(g2, IconUtils.ImageType.Cloud, new Point2D.Double(left + 37, y), GraphPanel.coreNetworkColor, new Dimension(75, 45));
//    Draw.drawString(g2, "Core Network", left + 37, y, Draw.X_POSITION.CENTER, Draw.Y_POSITION.CENTER);
//    y = y + objectspacing;
//    //IconUtils.paintAt(g2, IconUtils.ImageType.Cloud, new Point2D.Double(left + 37, y), GraphPanel.terminalNetworkColor, new Dimension(75, 45));
//    Draw.drawString(g2, "Edge Network", left + 37, y, Draw.X_POSITION.CENTER, Draw.Y_POSITION.CENTER);
//    y = y + objectspacing;
//    //IconUtils.paintAt(g2, IconUtils.ImageType.Router, new Point2D.Double(left + 18, y), GraphPanel.terminalNetworkColor, new Dimension(35, 35));
//    Draw.drawString(g2, "Router", left + 50, y, Draw.X_POSITION.LEFT, Draw.Y_POSITION.CENTER);
//    y = y + objectspacing;
//    //IconUtils.paintAt(g2, IconUtils.ImageType.Device, new Point2D.Double(left + 18, y), Colors.Black, new Dimension(20, 40));
//    Draw.drawString(g2, "Device", left + 50, y, Draw.X_POSITION.LEFT, Draw.Y_POSITION.CENTER);
  }
}
