package edu.umass.cs.gnrs.statusdisplay;

import edu.umass.cs.gnrs.util.Format;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Graphics;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JProgressBar;

/**
 *
 * @author westy
 */
public class StatusPane extends JPanel {

  private int id;
  private JLabel idLabel;
  private JLabel ipAddressLabel;
  private JLabel stateLabel;
  private JLabel statusLabel;
  private JLabel timeLabel;
//  private JLabel jLabel4;
//  private JLabel jLabel5;
  private JProgressBar progressBar;
  private JLabel progressBarHeaderLabel;

  /**
   * Creates new form StatusPane
   */
  public StatusPane(int id) {
    this.id = id;
    initComponents();
  }
  
  private static final int ADDRESSWIDTH = 350;
  private static final int PROGRESSWIDTH = 30;
  private static final int STATUSWIDTH = 700;

  /**
   * This method is called from within the constructor to initialize the form.
   */
  private void initComponents() {
    int height = 20;
    setLayout(new FlowLayout());
    idLabel = new JLabel();
    idLabel.setPreferredSize(new Dimension(40, height));
    ipAddressLabel = new JLabel();
    ipAddressLabel.setPreferredSize(new Dimension(ADDRESSWIDTH, height));
    if (id != -1) {
      progressBar = new JProgressBar();
      progressBar.setPreferredSize(new Dimension(PROGRESSWIDTH, height));
    } else {
      progressBarHeaderLabel = new JLabel();
      progressBarHeaderLabel.setPreferredSize(new Dimension(PROGRESSWIDTH, height));
    }
    stateLabel = new JLabel();
    stateLabel.setPreferredSize(new Dimension(100, height));
    statusLabel = new JLabel();
    statusLabel.setPreferredSize(new Dimension(STATUSWIDTH, height));
    timeLabel = new JLabel();
    timeLabel.setPreferredSize(new Dimension(110, height));
//    jLabel4 = new JLabel();
//    jLabel4.setPreferredSize(new Dimension(30, height));
//    jLabel5 = new JLabel();
//    jLabel5.setPreferredSize(new Dimension(30, height));

    if (id != -1) {
      idLabel.setText(Integer.toString(id));
    } else {
      idLabel.setText("#");
    }

    if (id != -1) {
      ipAddressLabel.setText("<host name unknown>");
    } else {
      ipAddressLabel.setText("Host Name");
    }

    if (id == -1) {
      stateLabel.setText("State");
      statusLabel.setText("Status");
      progressBarHeaderLabel.setText("");
      timeLabel.setText("Time");
    }

//    jLabel3.setText("X");
//
//    jLabel4.setText("Y");
//
//    jLabel5.setText("Z");
    add(idLabel);
    add(ipAddressLabel);
    if (id != -1) {
      StatusEntry entry = StatusModel.getInstance().getEntry(id);
      if (entry.getState() != StatusEntry.State.RUNNING && entry.getState() != StatusEntry.State.TERMINATED
              && entry.getState() != StatusEntry.State.ERROR) {
        add(progressBar);
      } else {
        ipAddressLabel.setPreferredSize(new Dimension(ADDRESSWIDTH + PROGRESSWIDTH + 1, height));
      }
    } else {
      add(progressBarHeaderLabel);
    }
    add(stateLabel);
    add(timeLabel);
    add(statusLabel);
//    add(jLabel4);
//    add(jLabel5);
  }

  @Override
  public void paintComponent(Graphics g) {
    if (id != -1) {
      StatusEntry entry = StatusModel.getInstance().getEntry(id);
      idLabel.setText(Integer.toString(id));
      stateLabel.setText(entry.getState().name());
      statusLabel.setText(entry.getStatusString());
      if (entry.getName() != null) {
        ipAddressLabel.setText(entry.getName());
      }
      // work around for a stupid obscure error: java.lang.IllegalAccessError: tried to access class defaultCharsetnrs.util.Format$7 from class edu.umass.cs.gnrs.util.Format
      //timeLabel.setText(Format.formatDateTimeOnlyMilleUTC(entry.getTime()));
      timeLabel.setText(entry.getTime().toString());
    }
    if (id != -1) {
      progressBar.setIndeterminate(true);
    }

  }
}
