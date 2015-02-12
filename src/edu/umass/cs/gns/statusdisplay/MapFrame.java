package edu.umass.cs.gns.statusdisplay;

//License: GPL. Copyright 2008 by Jan Peter Stotz
//Modified by Westy October 2012
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nsdesign.packet.Packet;
import edu.umass.cs.gns.nsdesign.packet.admin.TrafficStatusPacket;
import edu.umass.cs.gns.statusdisplay.StatusModel.SendNotation;
import edu.umass.cs.gns.util.Colors;
import edu.umass.cs.gns.util.Format;
import edu.umass.cs.gns.util.ScreenUtils;
import edu.umass.cs.gns.util.ThreadUtils;
import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.Point;
import java.awt.Rectangle;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JLayeredPane;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import org.westy.jmapviewer.Coordinate;
import org.westy.jmapviewer.JMapViewer;
import org.westy.jmapviewer.MapMarkerLabeledDot;
import org.westy.jmapviewer.MapPolylineImpl;
import org.westy.jmapviewer.OsmFileCacheTileLoader;
import org.westy.jmapviewer.OsmTileLoader;
import org.westy.jmapviewer.events.JMVCommandEvent;
import org.westy.jmapviewer.interfaces.JMapViewerEventListener;
import org.westy.jmapviewer.interfaces.TileLoader;
import org.westy.jmapviewer.interfaces.TileSource;
import org.westy.jmapviewer.tilesources.BingAerialTileSource;
import org.westy.jmapviewer.tilesources.MapQuestOpenAerialTileSource;
import org.westy.jmapviewer.tilesources.MapQuestOsmTileSource;
import org.westy.jmapviewer.tilesources.OsmTileSource;

/**
 * TODO add marker with label fix color and line issues
 */
@SuppressWarnings("unchecked")
public class MapFrame extends JFrame implements JMapViewerEventListener, UpdateListener {

  private static final long serialVersionUID = 1L;
  private JMapViewer map = null;
  private JLabel zoomLabel = null;
  private JLabel zoomValue = null;
  private JLabel mperpLabelName = null;
  private JLabel mperpLabelValue = null;
  //
  private static MapFrame instance = null;

  public static MapFrame getInstance() {
    if (instance == null) {
      instance = new MapFrame();
    }
    return instance;
  }

  public class TimeComparator implements Comparator<SendNotation> {

    @Override
    public int compare(SendNotation o1, SendNotation o2) {
      return o1.getTime().compareTo(o2.getTime());
    }
  }
  int cutoffSeconds = 10;

  @Override
  public synchronized void update() {
    if (instance != null) {
      StatusModel model = StatusModel.getInstance();
      //System.out.println(model);
      map.removeAllMapMarkers();
      TreeSet<StatusEntry> entries = new TreeSet<StatusEntry>(model.getEntries());
      for (StatusEntry entry : entries) {
        if (entry.getLocation() != null) {
          map.addMapMarker(new MapMarkerLabeledDot(entry.getId().toString(), statusColor(entry), entry.getLocation().getY(), entry.getLocation().getX()));
        }
      }
      map.removeAllMapPolylines();
      List<StatusModel.SendNotation> sendNotations = model.getSendNotations();
      if (!sendNotations.isEmpty()) {
        Collections.sort(sendNotations, Collections.reverseOrder(new TimeComparator()));
        Date latest = sendNotations.get(0).getTime();
        Date cutoffTime = new Date(latest.getTime() - cutoffSeconds * 1000L);
        boolean oneShown = false;
        for (StatusModel.SendNotation notation : sendNotations) {
          // always show one
          if (!oneShown || notation.getTime().after(cutoffTime)) {
            oneShown = true;
            //System.out.println("Cutoff: " + Format.formatDateTimeOnlyMilleUTC(cutoffTime) + " time: " + Format.formatDateTimeOnlyMilleUTC(notation.getTime()));
            StatusEntry from = StatusModel.getInstance().getEntry(notation.getSender());
            StatusEntry to = StatusModel.getInstance().getEntry(notation.getReceiver());
            if (from.getLocation() != null && to.getLocation() != null) {
              Coordinate fromCoord = new Coordinate(from.getLocation().getY(), from.getLocation().getX());
              Coordinate toCoord = new Coordinate(to.getLocation().getY(), to.getLocation().getX());
              List<Coordinate> coords = new ArrayList<Coordinate>(Arrays.asList(fromCoord, toCoord));
              map.addMapPolyline(new MapPolylineImpl(coords,
                      connectionColor(notation.getPortType(), notation.getPacketType()),
                      new BasicStroke(2.0f), true,
                      10, 10, 0,
                      connectionOffset(notation.getPortType(), notation.getPacketType()),
                      notation.getName() != null ? (notation.getName().substring(0, Math.min(notation.getName().length(), 10)) + " / " + notation.getKey()) : null));
            }
          }
        }
      }
    }
  }

  private Color statusColor(StatusEntry entry) {
    switch (entry.getState()) {
      case INITIALIZING:
        return Color.GRAY;
      case PENDING:
        return Color.YELLOW;
      case RUNNING:
        return Color.GREEN;
      case ERROR:
        return Color.RED;
      case TERMINATED:
        return Color.PINK;
      default:
        return Color.BLACK;
    }
  }

  public static class PacketGraphic {

    private Color color;
    private double offset;

    public PacketGraphic(Color color, double offset) {
      this.color = color;
      this.offset = offset;
    }

    public Color getColor() {
      return color;
    }

    public double getOffset() {
      return offset;
    }
  }
  public static final Map<Packet.PacketType, PacketGraphic> packetGraphics = new EnumMap(Packet.PacketType.class);

  static {
    packetGraphics.put(Packet.PacketType.DNS_SUBTYPE_QUERY, new PacketGraphic(Colors.Black, 1.0));
    packetGraphics.put(Packet.PacketType.DNS_SUBTYPE_RESPONSE, new PacketGraphic(Colors.Blue, 1.1));
    packetGraphics.put(Packet.PacketType.DNS_SUBTYPE_ERROR_RESPONSE, new PacketGraphic(Colors.Red, 1.2));
    packetGraphics.put(Packet.PacketType.ADD_RECORD, new PacketGraphic(Colors.Green, 1.3));
   // packetGraphics.put(Packet.PacketType.REPLICATE_RECORD, new PacketGraphic(Colors.Cyan, 1.4));
    //packetGraphics.put(Packet.PacketType.REMOVE_REPLICATION_RECORD, new PacketGraphic(Colors.DarkSalmon, 1.0));
    //packetGraphics.put(Packet.PacketType.NAME_RECORD_STATS_REQUEST, new PacketGraphic(Colors.Grey, 1.0));
    //packetGraphics.put(Packet.PacketType.NAME_RECORD_STATS_RESPONSE, new PacketGraphic(Colors.BlueViolet, 1.0));

//    packetGraphics.put(Packet.PacketType.UPDATE_NAMESERVER, new PacketGraphic(Colors.YellowGreen, 1.0));
//    packetGraphics.put(Packet.PacketType.ACTIVE_NAMESERVER_UPDATE, new PacketGraphic(Colors.OliveDrab, 1.0));

    packetGraphics.put(Packet.PacketType.NAMESERVER_SELECTION, new PacketGraphic(Colors.MediumSeaGreen, 1.0));
    packetGraphics.put(Packet.PacketType.UPDATE, new PacketGraphic(Colors.MediumPurple, 1.0));
    //packetGraphics.put(Packet.PacketType.UPDATE_ADDRESS_NS, new PacketGraphic(Colors.Magenta, 1.0));
    packetGraphics.put(Packet.PacketType.UPDATE_CONFIRM, new PacketGraphic(Colors.MediumGreen, 1.0));



    //packetGraphics.put(Packet.PacketType.ACTIVE_NAMESERVER_INFO, new PacketGraphic(Colors.DarkOrange, 1.0));

    //packetGraphics.put(Packet.PacketType.DUMP_REQUEST, new PacketGraphic(Colors.Black, 1.0));
    //packetGraphics.put(Packet.PacketType.STATUS, new PacketGraphic(Colors.Black, 1.0));
    //packetGraphics.put(Packet.PacketType.TRAFFIC_STATUS, new PacketGraphic(Colors.Black, 1.0));
    //packetGraphics.put(Packet.PacketType.STATUS_INIT, new PacketGraphic(Colors.Black, 1.0));
    //packetGraphics.put(Packet.PacketType.SENTINAL, new PacketGraphic(Colors.Black, 1.0));

  }

  private double connectionOffset(GNS.PortType portType, Packet.PacketType packetType) {
    PacketGraphic pg = packetGraphics.get(packetType);
    if (pg != null) {
      return pg.getOffset();
    }
    return 0;
  }

  private Color connectionColor(GNS.PortType portType, Packet.PacketType packetType) {
    PacketGraphic pg = packetGraphics.get(packetType);
    if (pg != null) {
      return pg.getColor();
    }
    switch (portType) {
      case NS_TCP_PORT:
        return Color.RED;
      case NS_ADMIN_PORT:
        return Colors.DarkGoldenrod;
      default:
        return Color.BLACK;
    }
  }

  private MapFrame() {
    super("GNRS Map");
    setSize(400, 400);
    initComponents();
  }
  KeyPanel keyPanel;

  private void initComponents() {
    map = new JMapViewer();

    // Listen to the map viewer for user operations so components will
    // recieve events and update
    map.addJMVListener(this);

    // final JMapViewer map = new JMapViewer(new MemoryTileCache(),4);
    // map.setTileLoader(new OsmFileCacheTileLoader(map));
    // new DefaultMapController(map);

    setLayout(new BorderLayout());
    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    //setExtendedState(JFrame.MAXIMIZED_BOTH);
    JPanel panel = new JPanel();
    JPanel helpPanel = new JPanel();

    mperpLabelName = new JLabel("Meters/Pixels: ");
    mperpLabelValue = new JLabel(String.format("%s", map.getMeterPerPixel()));

    zoomLabel = new JLabel("Zoom: ");
    zoomValue = new JLabel(String.format("%s", map.getZoom()));

    add(panel, BorderLayout.NORTH);
    add(helpPanel, BorderLayout.SOUTH);
    JLabel helpLabel = new JLabel("Use right mouse button to move,\n "
            + "left double click or mouse wheel to zoom.");
    helpPanel.add(helpLabel);
    JButton button = new JButton("setDisplayToFitMapMarkers");
    button.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        map.setDisplayToFitMapMarkers();
      }
    });
    JComboBox tileSourceSelector = new JComboBox(new TileSource[]{new OsmTileSource.Mapnik(),
              new OsmTileSource.CycleMap(), new BingAerialTileSource(), new MapQuestOsmTileSource(), new MapQuestOpenAerialTileSource()});
    tileSourceSelector.addItemListener(new ItemListener() {
      @Override
      public void itemStateChanged(ItemEvent e) {
        map.setTileSource((TileSource) e.getItem());
      }
    });
    JComboBox tileLoaderSelector;
    try {
      tileLoaderSelector = new JComboBox(new TileLoader[]{new OsmFileCacheTileLoader(map),
                new OsmTileLoader(map)});
    } catch (IOException e) {
      tileLoaderSelector = new JComboBox(new TileLoader[]{new OsmTileLoader(map)});
    }
    tileLoaderSelector.addItemListener(new ItemListener() {
      @Override
      public void itemStateChanged(ItemEvent e) {
        map.setTileLoader((TileLoader) e.getItem());
      }
    });
    map.setTileLoader((TileLoader) tileLoaderSelector.getSelectedItem());
    panel.add(tileSourceSelector);
    panel.add(tileLoaderSelector);
    final JCheckBox showMapMarker = new JCheckBox("Map markers visible");
    showMapMarker.setSelected(map.getMapMarkersVisible());
    showMapMarker.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        map.setMapMarkerVisible(showMapMarker.isSelected());
        map.setMapPolygonsVisible(showMapMarker.isSelected());
        map.setMapRectanglesVisible(showMapMarker.isSelected());
        map.setMapPolylinesVisible(showMapMarker.isSelected());
      }
    });
    panel.add(showMapMarker);
    final JCheckBox showTileGrid = new JCheckBox("Tile grid visible");
    showTileGrid.setSelected(map.isTileGridVisible());
    showTileGrid.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        map.setTileGridVisible(showTileGrid.isSelected());
      }
    });
    panel.add(showTileGrid);
//    final JCheckBox showZoomControls = new JCheckBox("Show zoom controls");
//    showZoomControls.setSelected(map.getZoomContolsVisible());
//    showZoomControls.addActionListener(new ActionListener() {
//      @Override
//      public void actionPerformed(ActionEvent e) {
//        map.setZoomContolsVisible(showZoomControls.isSelected());
//      }
//    });
//    panel.add(showZoomControls);
    map.setZoomContolsVisible(false);
    panel.add(button);

    panel.add(zoomLabel);
    panel.add(zoomValue);
    panel.add(mperpLabelName);
    panel.add(mperpLabelValue);


    add(map, BorderLayout.CENTER);
    initializeKeyPanel();
    initializeSlider();


  }

  private void initializeKeyPanel() {
    keyPanel = new KeyPanel();
    keyPanel.setVisible(true);

    getLayeredPane().add(keyPanel, JLayeredPane.PALETTE_LAYER);

    this.addComponentListener(new ComponentAdapter() {
      @Override
      public void componentResized(ComponentEvent e) {
        // This is only called when the user releases the mouse button.
        Rectangle bounds = getBounds();
        GNS.getLogger().finer("componentResized, BOUNDS is " + bounds);
        sizeComponents();
      }
    });
  }
  private static final int KEYPANELWIDTH = 240;
  private static final int KEYPANELHEIGHT = 240;
  private static final int BOTTOMMARGIN = 240;

  private void sizeComponents() {
    Rectangle bounds = map.getBounds();
    Point location = map.getLocation();
    keyPanel.setBounds((int) (location.getX() + bounds.getWidth() - KEYPANELWIDTH), (int) (location.getY() + bounds.getHeight() - KEYPANELHEIGHT),
            KEYPANELWIDTH, KEYPANELHEIGHT);
  }
  private int zoom;
  private JLabel topLabel;
  private JSlider slider;
  private JLabel label;
  public static Font font = new Font("Arial", Font.PLAIN, 14);
  public static Font smallFont = new Font("Arial", Font.PLAIN, 12);

  private void initializeSlider() {
    topLabel = new JLabel("<html><div WIDTH=25 align=CENTER>Time Shown</div><html>");
    topLabel.setFont(smallFont);
    topLabel.setBounds(12, 45, 40, 25);
    getLayeredPane().add(topLabel, JLayeredPane.PALETTE_LAYER);
    slider = new JSlider(0, 60, cutoffSeconds);
    slider.setOrientation(JSlider.VERTICAL);
    slider.setBounds(15, 60, 30, 150);
    slider.setOpaque(false);
    slider.addChangeListener(new ChangeListener() {
      @Override
      public void stateChanged(ChangeEvent e) {
        cutoffSeconds = slider.getValue();
        label.setText(Integer.toString(cutoffSeconds) + "s");
        update();
      }
    });
    getLayeredPane().add(slider, JLayeredPane.PALETTE_LAYER);
    label = new JLabel(Integer.toString(cutoffSeconds) + "s");
    label.setFont(font);
    label.setBounds(18, 205, 23, 18);
    getLayeredPane().add(label, JLayeredPane.PALETTE_LAYER);

  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    java.awt.EventQueue.invokeLater(new Runnable() {
      @Override
      public void run() {
        //
        StatusFrame.getInstance().setVisible(true);
        StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());

        MapFrame.getInstance().setVisible(true);
        ScreenUtils.putOnWidestScreen(MapFrame.getInstance());
        StatusModel.getInstance().addUpdateListener(MapFrame.getInstance());

      }
    });
    //test();
  }

  private static void test() {
//    java.awt.EventQueue.invokeLater(new Runnable() {
//      @Override
//      public void run() {
//        //
//        StatusFrame.getInstance().setVisible(true);
//        StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());
//
//        MapFrame.getInstance().setVisible(true);
//        ScreenUtils.putOnWidestScreen(MapFrame.getInstance());
//        StatusModel.getInstance().addUpdateListener(MapFrame.getInstance());
//
//      }
//    });
    //
    Random rand = new Random();
    ThreadUtils.sleep(4000);
    for (int i = 1; i < 51; i++) {
      StatusModel.getInstance().queueAddEntry(Integer.toString(i));
      double randomLat = (double) (rand.nextInt(180) - 90);
      double randomLon = (double) (rand.nextInt(360) - 180);
      StatusModel.getInstance().queueUpdate(Integer.toString(i), i + "stuff", "10.0.0." + i, new Point2D.Double(randomLon, randomLat));
    }

    for (int i = 1; i < 51; i++) {
      Object from = Integer.toString(rand.nextInt(50) + 1);
      Object to = Integer.toString(rand.nextInt(50) + 1);
      switch (rand.nextInt(7)) {
        case 0:
          //StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.REPLICATE_RECORD, null, null));
          break;
        case 1:
          StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.DNS_SUBTYPE_QUERY, "E592EDC0DAD5E4DF8E5E79F22BAB6680D1899567", null));
          StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(to, from, GNS.PortType.NS_TCP_PORT, Packet.PacketType.DNS_SUBTYPE_ERROR_RESPONSE, "E592EDC0DAD5E4DF8E5E79F22BAB6680D1899567", null));
          StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(to, from, GNS.PortType.NS_TCP_PORT, Packet.PacketType.DNS_SUBTYPE_RESPONSE, "E592EDC0DAD5E4DF8E5E79F22BAB6680D1899567", null));
          break;
        case 2:
          StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.DNS_SUBTYPE_RESPONSE, null, null));
          break;
        case 3:
          //StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.UPDATE_ADDRESS_NS, null, null));
          break;
        case 4:
          //StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.NAME_RECORD_STATS_RESPONSE, null, null));
          break;
        case 5:
          StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.ADD_RECORD, null, null));
          break;
        case 6:
          StatusModel.getInstance().queueSendNotation(new TrafficStatusPacket(from, to, GNS.PortType.NS_TCP_PORT, Packet.PacketType.DNS_SUBTYPE_ERROR_RESPONSE, null, null));
          break;
      }
      ThreadUtils.sleep(100);
    }
  }

  private void updateZoomParameters() {
    if (mperpLabelValue != null) {
      mperpLabelValue.setText(Format.formatFloat(map.getMeterPerPixel()));
    }
    if (zoomValue != null) {
      zoomValue.setText(String.format("%s", map.getZoom()));
    }
  }

  @Override
  public void processCommand(JMVCommandEvent command) {
    if (command.getCommand().equals(JMVCommandEvent.COMMAND.ZOOM)
            || command.getCommand().equals(JMVCommandEvent.COMMAND.MOVE)) {
      updateZoomParameters();
    }
  }
}
