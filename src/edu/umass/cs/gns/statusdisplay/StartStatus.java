package edu.umass.cs.gns.statusdisplay;

import edu.umass.cs.gns.nsdesign.GNSNodeConfig;
//import edu.umass.cs.gns.util.ConfigFileInfo;
import edu.umass.cs.gns.util.GEOLocator;
import edu.umass.cs.gns.util.HostInfo;
import edu.umass.cs.gns.util.ScreenUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * A helper class that does all the necessary things to get status windows up and running.
 * 
 * @author westy
 */
public class StartStatus {

  private static Options commandLineOptions;
  private static GNSNodeConfig nodeConfig;

  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option nsFile = OptionBuilder.withArgName("file").hasArg().withDescription("Name server file").create("nsfile");
    //Option lnsid = OptionBuilder.withArgName("lnsid").hasArg().withDescription("Local name server id").create("lnsid");
    //Option local = new Option("local", "all servers are on this machine");
    commandLineOptions = new Options();
    commandLineOptions.addOption(nsFile);
    //commandLineOptions.addOption(lnsid);
    //commandLineOptions.addOption(local);
    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  /**
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    nodeConfig = null;
    try {
      CommandLine parser = initializeOptions(args);
      String nsFile = parser.getOptionValue("nsfile");
      //lnsid = Integer.parseInt(parser.getOptionValue("lnsid"));
      nodeConfig = new GNSNodeConfig(nsFile, 0);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    //
    //Set<String> hosts = new HashSet<String>();
    for (int id : nodeConfig.getNodeIDs()) {
      StatusModel.getInstance().queueAddEntry(id);
    }
    //
    try {
      new StatusListener().start();
    } catch (IOException e) {
      System.out.println("Unable to start Status Listener: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
    //
    StatusListener.sendOutServerInitPackets(nodeConfig, nodeConfig.getNodeIDs());
    //
    java.awt.EventQueue.invokeLater(new Runnable() {
      @Override
      public void run() {
        //StatusFrame.getInstance().setVisible(true);
        //StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());
        MapFrame.getInstance().setVisible(true);
        ScreenUtils.putOnWidestScreen(MapFrame.getInstance());
        StatusModel.getInstance().addUpdateListener(MapFrame.getInstance());
      }
    });
//    StatusFrame.getInstance().setVisible(true);
//    StatusModel.getInstance().addUpdateListener(StatusFrame.getInstance());
//    MapFrame.getInstance().setVisible(true);
//    StatusModel.getInstance().addUpdateListener(MapFrame.getInstance());
    
    ArrayList<String> cities = new ArrayList<String>();
    cities.add("Arlington, VA, USA");
    cities.add("Seattle, WA, USA");
    cities.add("Tokyo, Japan");
    cities.add("Dublin, Ireland");
    cities.add("Sal Palo, Brazil");
    cities.add("Dallas, TX, USA");
    cities.add("Munich, Germany");
    cities.add("Paris, France");
    cities.add("Boston, MA, USA");
    cities.add("San Diego, CA, USA");
    cities.add("Perth, Australia");
    cities.add("Singapore");
    cities.add("Cape Town, South Africa");
    cities.add("Bhopal India");
    cities.add("Istanbul, Turkey");
    cities.add("Moscow, Russia");
    cities.add("Tehran, Iran");
    cities.add("Riyadh, Saudi Arabia");
    cities.add("Morocco");
    
    for (int id : nodeConfig.getNodeIDs()) {
      HostInfo info = nodeConfig.getHostInfo(id);
      InetAddress ipAddress = info.getIpAddress();
      StatusModel.getInstance().queueUpdate(id, StatusEntry.State.RUNNING);
//      Random rand = new Random();
//      double randomLat = (double) (rand.nextInt(180) - 90);
//      double randomLon = (double) (rand.nextInt(360) - 180);
      StatusModel.getInstance().queueUpdate(id, "Node " + id, ipAddress.getHostAddress(),
              GEOLocator.lookupCityLocation(cities.get(id))
              //new Point2D.Double(randomLon, randomLat) 
              //IPLocator.lookupLocation(ipAddress.getHostAddress())
              );
    }
  }
}