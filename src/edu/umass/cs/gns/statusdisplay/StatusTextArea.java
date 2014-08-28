package edu.umass.cs.gns.statusdisplay;

import edu.umass.cs.gns.localnameserver.httpserver.Defs;
import edu.umass.cs.gns.util.Format;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Graphics;
import javax.swing.JLabel;
import javax.swing.JProgressBar;
import javax.swing.JTextArea;

/**
 *
 * @author westy
 */
public class StatusTextArea extends JTextArea {

  private final static String NEWLINE = System.getProperty("line.separator");

  public StatusTextArea() {
    updateTextArea();
  }

  private void updateTextArea() {
    StringBuilder text = new StringBuilder();
    text.append("Notations:");
    text.append(NEWLINE);
    for (StatusModel.SendNotation notation : StatusModel.getInstance().getSendNotations()) {
      text.append(notation.toString());
      text.append(NEWLINE);
    }
    setText(text.toString());
  }

//  @Override
//  public void paintComponent(Graphics g) {
//    updateTextArea();
//  }
}
