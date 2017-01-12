/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): Westy
 *
 */
package edu.umass.cs.aws.support;

import edu.umass.cs.aws.networktools.Pinger;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.Address;
import com.amazonaws.services.ec2.model.AssociateAddressRequest;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.DescribeAddressesResult;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.ImportKeyPairRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.Volume;
import edu.umass.cs.gnsserver.main.GNSConfig;
import edu.umass.cs.gnscommon.utils.ThreadUtils;
import edu.umass.cs.aws.networktools.ExecuteBash;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 *
 * @author westy
 */
public class AWSEC2 {

  /*
   *            AwsCredentials.properties file before you try to run this
   *            sample.
   * http://aws.amazon.com/security-credentials
   */

  /**
   *
   */
  public static final String DEFAULT_SECURITY_GROUP_NAME = "aws";

  private static String currentTab = "";
  private static final List<String> IPRANGESALL = new ArrayList<>(Arrays.asList("0.0.0.0/0"));
  private static final int HTTPPORT = 80;
  private static final int HTTPSPORT = 443;
  private static final int HTTPNONROOTPORT = 8080;
  private static final int SSHPORT = 22;
  private static final int MYSQLPORT = 3306;
  private static final int ECHOTYPE = 8;
  private static final int WILDCARDCODE = -1;
  private static final String TCPPROTOCOL = "tcp";
  private static final String UDPPROTOCOL = "udp";
  private static final String ICMPPROTOCOL = "icmp";
  //
  private static final String NEWLINE = System.getProperty("line.separator");
  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String PRIVATEKEYFILEEXTENSION = ".pem";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";

  /**
   * Set the EC2 Region. All commands use the current region.
   *
   * @param ec2
   * @param region
   */
  public static void setRegion(AmazonEC2 ec2, RegionRecord region) {
    ec2.setEndpoint(region.getURL());
  }

  /**
   * Describe Availability Zones
   *
   * @param ec2
   */
  public static void describeAvailabilityZones(AmazonEC2 ec2) {
    StringBuilder output = new StringBuilder();
    String prefix = currentTab + "Availability Zones: ";
    DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
    prefix = prefix.concat(" [" + availabilityZonesResult.getAvailabilityZones().size() + " total] ");
    for (AvailabilityZone zone : availabilityZonesResult.getAvailabilityZones()) {
      output.append(prefix);
      prefix = ", ";
      output.append(zone.getZoneName());
    }
    System.out.println(output);
  }

  /**
   * Returns a list of strings of all the availability zones in the current region.
   *
   * @param ec2
   * @return a list of zone strings
   */
  public static List<String> getAvailabilityZones(AmazonEC2 ec2) {
    ArrayList<String> result = new ArrayList<>();
    DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
    for (AvailabilityZone zone : availabilityZonesResult.getAvailabilityZones()) {
      result.add(zone.getZoneName());
    }
    return result;
  }

  /**
   * Describe Security Groups
   *
   * @param ec2
   */
  public static void describeSecurityGroups(AmazonEC2 ec2) {
    StringBuilder output = new StringBuilder();
    String prefix = currentTab + "Security Groups: ";
    DescribeSecurityGroupsResult describeSecurityGroupsResult = ec2.describeSecurityGroups();
    for (SecurityGroup securityGroup : describeSecurityGroupsResult.getSecurityGroups()) {
      output.append(prefix);
      prefix = ", ";
      output.append(securityGroup.getGroupName());
    }
    System.out.println(output);
  }

  /**
   * Describe Key Pairs
   *
   * @param ec2
   */
  public static void describeKeyPairs(AmazonEC2 ec2) {
    StringBuilder output = new StringBuilder();
    String prefix = currentTab + "Key Pairs: ";
    DescribeKeyPairsResult dkr = ec2.describeKeyPairs();
    for (KeyPairInfo keyPairInfo : dkr.getKeyPairs()) {
      output.append(prefix);
      prefix = ", ";
      output.append(keyPairInfo.getKeyName());
    }
    System.out.println(output);
  }

  /**
   * Describe Current Instances
   *
   * @param ec2
   */
  public static void describeInstances(AmazonEC2 ec2) {
    StringBuilder output = new StringBuilder();
    String prefix = currentTab + "Current Instances: ";
    DescribeInstancesResult describeInstancesResult = ec2.describeInstances();
    List<Reservation> reservations = describeInstancesResult.getReservations();
    Set<Instance> instances = new HashSet<Instance>();
    // add all instances to a Set.
    for (Reservation reservation : reservations) {
      instances.addAll(reservation.getInstances());
    }
    prefix = prefix.concat(" [" + instances.size() + " total] ");

    for (Instance ins : instances) {
      // instance state
      output.append(prefix);
      prefix = ", ";
      output.append(ins.getPublicDnsName());
      output.append(" (");
      output.append(ins.getInstanceId());
      output.append(") ");
      output.append(ins.getState().getName());
    }
    System.out.println(output);
  }

  /**
   * Create a New Security Group with our standard permissions
   *
   * @param ec2
   * @param name
   * @return the name of the new group
   */
  public static String createSecurityGroup(AmazonEC2 ec2, String name) {
    CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(name, name + " security group");
    ec2.createSecurityGroup(securityGroupRequest);
    AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest();
    ingressRequest.setGroupName(name);

    List<IpPermission> permissions = new ArrayList<>();

    // open up ping (echo request)
    permissions.add(new IpPermission()
            .withIpProtocol(ICMPPROTOCOL)
            .withFromPort(ECHOTYPE)
            .withToPort(WILDCARDCODE)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(TCPPROTOCOL)
            .withFromPort(SSHPORT)
            .withToPort(SSHPORT)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(TCPPROTOCOL)
            .withFromPort(HTTPPORT)
            .withToPort(HTTPPORT)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(TCPPROTOCOL)
            .withFromPort(HTTPNONROOTPORT)
            .withToPort(HTTPNONROOTPORT)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(TCPPROTOCOL)
            .withFromPort(HTTPSPORT)
            .withToPort(HTTPSPORT)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(TCPPROTOCOL)
            .withFromPort(MYSQLPORT)
            .withToPort(MYSQLPORT)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(TCPPROTOCOL)
            .withFromPort(20000)
            .withToPort(30000)
            .withIpRanges(IPRANGESALL));
    permissions.add(new IpPermission()
            .withIpProtocol(UDPPROTOCOL)
            .withFromPort(20000)
            .withToPort(30000)
            .withIpRanges(IPRANGESALL));

    ingressRequest.setIpPermissions(permissions);
    ec2.authorizeSecurityGroupIngress(ingressRequest);
    return name;
  }

  /**
   * Returns the name of an existing security group of the given name or null if one does not exist.
   *
   * @param ec2
   * @param name
   * @return the same name if it exists, null otherwise
   */
  public static SecurityGroup findSecurityGroup(AmazonEC2 ec2, String name) {
    DescribeSecurityGroupsResult describeSecurityGroupsResult = ec2.describeSecurityGroups();
    for (SecurityGroup securityGroup : describeSecurityGroupsResult.getSecurityGroups()) {
      if (name.equals(securityGroup.getGroupName())) {
        System.out.println("Found security group " + name);
        return securityGroup;
      }
    }
    return null;
  }

  /**
   * Returns an existing security group of the given name or creates one if it does not exist.
   *
   * @param ec2
   * @param name
   * @return the name of the group
   */
  public static SecurityGroup findOrCreateSecurityGroup(AmazonEC2 ec2, String name) {
    SecurityGroup result = findSecurityGroup(ec2, name);
    if (result == null) {
      createSecurityGroup(ec2, name);
      System.out.println("Created security group " + name);
    }
    return findSecurityGroup(ec2, name);
  }

  /**
   * Create a New Key Pair
   *
   * @param ec2
   * @param name
   * @return the keypair
   */
  public static KeyPair createKeyPair(AmazonEC2 ec2, String name) {
    CreateKeyPairRequest newKeyRequest = new CreateKeyPairRequest();
    newKeyRequest.setKeyName(name);
    CreateKeyPairResult keyresult = ec2.createKeyPair(newKeyRequest);

    /**
     * **********************print the properties of this key****************
     */
    KeyPair keyPair = keyresult.getKeyPair();
    System.out.println("The key we created is = " + keyPair.toString());

    /**
     * ***************store the key in a .pem file ***************
     */
    try {
      String fileName = KEYHOME + FILESEPARATOR + name + PRIVATEKEYFILEEXTENSION;
      File distFile = new File(fileName);
      BufferedReader bufferedReader = new BufferedReader(new StringReader(keyPair.getKeyMaterial()));
      BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(distFile));
      char buf[] = new char[1024];
      int len;
      while ((len = bufferedReader.read(buf)) != -1) {
        bufferedWriter.write(buf, 0, len);
      }
      bufferedWriter.flush();
      bufferedReader.close();
      bufferedWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return keyPair;
  }

  /**
   *
   * @param ec2
   * @param name
   * @param publicKeyMaterial
   */
  public static void importKeyPair(AmazonEC2 ec2, String name, String publicKeyMaterial) {
    ImportKeyPairRequest newKeyRequest = new ImportKeyPairRequest(name, publicKeyMaterial);
    ec2.importKeyPair(newKeyRequest);
  }

  /**
   * Returns the name of an existing key pair of the given name or null if it does not exist.
   *
   * @param ec2
   * @param name
   * @return the name of the keypair or null
   */
  public static String findKeyPairInfo(AmazonEC2 ec2, String name) {
    DescribeKeyPairsResult dkr = ec2.describeKeyPairs();
    for (KeyPairInfo keyPairInfo : dkr.getKeyPairs()) {
      if (name.equals(keyPairInfo.getKeyName())) {
        System.out.println("Found key pair " + name);
        return name;
      }
    }
    return null;
  }

  /**
   * Returns the name of an existing key pair of the given name or creates one if it does not exist.
   *
   * @param ec2
   * @param name
   * @return the name of the keypair or null
   */
  public static String findOrCreateKeyPair(AmazonEC2 ec2, String name) {
    String result = findKeyPairInfo(ec2, name);
    if (result == null) {
      File publicKeyFile = new File(KEYHOME + FILESEPARATOR + name + ".pub");
      if (publicKeyFile.exists()) {
        importKeyPair(ec2, name, readPublicKeyMaterial(publicKeyFile));
        System.out.println("Imported key pair " + name + " from " + publicKeyFile.getPath());
      } else {
        createKeyPair(ec2, name);
        System.out.println("Created key pair " + name);
      }
      result = name;
    }
    return result;
  }

  private static String readPublicKeyMaterial(File file) {
    try {
      byte[] b;
      try (InputStream in = new FileInputStream(file)) {
        b = new byte[(int) file.length()];
        int len = b.length;
        int total = 0;
        while (total < len) {
          int result = in.read(b, total, len - total);
          if (result == -1) {
            break;
          }
          total += result;
        }
      }
      return new String(b, "UTF-8");
    } catch (IOException e) {
      System.out.println("Problem reading key material: " + e);
      e.printStackTrace();
    }

    return null;
    // JAVA7 return new String(Files.readAllBytes(FileSystems.getDefault().getPath(filename)));
  }

  /**
   *
   * @param ec2
   */
  public static void describeElasticIPs(AmazonEC2 ec2) {
    StringBuilder output = new StringBuilder();
    String prefix = currentTab + "Elastic IPs: ";
    DescribeAddressesResult describeAddressesResult = ec2.describeAddresses();
    for (Address address : describeAddressesResult.getAddresses()) {
      output.append(prefix);
      prefix = ", ";
      output.append(address.getPublicIp());
    }
    System.out.println(output);
  }

  /**
   *
   * @param ec2
   * @param ip
   * @return the address
   */
  public static Address findElasticIP(AmazonEC2 ec2, String ip) {
    DescribeAddressesResult describeAddressesResult = ec2.describeAddresses();
    for (Address address : describeAddressesResult.getAddresses()) {
      if (address.getPublicIp().equals(ip)) {
        return address;
      }
    }
    return null;
  }

  /**
   *
   * @param ec2
   * @param ip
   * @param instance
   */
  public static void associateAddress(AmazonEC2 ec2, String ip, Instance instance) {
    Address address;
    if ((address = findElasticIP(ec2, ip)) != null) {
      if (address.getDomain().equals("vpc")) {
        System.out.println("VPC Elastic IP:  " + ip);
        ec2.associateAddress(new AssociateAddressRequest()
                .withInstanceId(instance.getInstanceId())
                .withAllocationId(address.getAllocationId()));
      } else {
        System.out.println("EC2 Classic Elastic IP:  " + ip);
        ec2.associateAddress(new AssociateAddressRequest(instance.getInstanceId(), ip));
      }
    }
  }

  /**
   * Create an Instance
   *
   * @param ec2
   * @param amiRecord
   * @param key
   * @param securityGroup
   * @return the instanceID string
   */
  public static String createInstanceAndWait(AmazonEC2 ec2, AMIRecord amiRecord, String key, SecurityGroup securityGroup) {
    RunInstancesRequest runInstancesRequest;
    if (amiRecord.getVpcSubnet() != null) {
      System.out.println("subnet: " + amiRecord.getVpcSubnet() + " securityGroup: " + securityGroup.getGroupName());
      // new VPC
      runInstancesRequest = new RunInstancesRequest()
              .withMinCount(1)
              .withMaxCount(1)
              .withImageId(amiRecord.getName())
              .withInstanceType(amiRecord.getInstanceType())
              .withKeyName(key)
              .withSubnetId(amiRecord.getVpcSubnet())
              .withSecurityGroupIds(Arrays.asList(securityGroup.getGroupId()));
    } else {
      runInstancesRequest = new RunInstancesRequest(amiRecord.getName(), 1, 1);
      runInstancesRequest.setInstanceType(amiRecord.getInstanceType());
      runInstancesRequest.setSecurityGroups(new ArrayList<>(Arrays.asList(securityGroup.getGroupName())));
      runInstancesRequest.setKeyName(key);
    }

    RunInstancesResult runInstancesResult = ec2.runInstances(runInstancesRequest);

    Instance instance = runInstancesResult.getReservation().getInstances().get(0);
    String createdInstanceId = instance.getInstanceId();

    System.out.println("Waiting for instance " + amiRecord.getName() + " to start");
    long startTime = System.currentTimeMillis();
    do {
      ThreadUtils.sleep(1000);
      if (System.currentTimeMillis() - startTime > 90000) {// give it a minute and a half
        System.out.println(createdInstanceId + " timed out waiting for start.");
        return null;
      }
      // regrab the instance data from the server
      instance = findInstance(ec2, createdInstanceId);

      //System.out.print(instance.getState().getName());
      System.out.print(".");

    } while (instance != null && !instance.getState().getName().equals(InstanceStateRecord.RUNNING.getName()));
    System.out.println();

    return createdInstanceId;
  }

  /**
   *
   */
  public static String DEFAULTSECONDMOUNTPOINT = "/dev/sda2";

  /**
   * Creates a volume and attaches and mounts it on the instance at the default secondary mount point
   * DEFAULTSECONDMOUNTPOINT. SO DON'T CALL THIS TWICE ON THE SAME INSTANCE!!
   *
   * @param ec2
   * @param instanceId
   * @return the id of the volume
   */
  public static String createAndAttachVolume(AmazonEC2 ec2, String instanceId) {
    return createAndAttachVolume(ec2, instanceId, DEFAULTSECONDMOUNTPOINT);
  }

  /**
   * Creates a volume and attaches and mounts it on the instance at the specified mount point.
   *
   * @param ec2
   * @param instanceId
   * @param mountPoint
   * @return the id of the volume
   */
  public static String createAndAttachVolume(AmazonEC2 ec2, String instanceId, String mountPoint) {
    // ATTACH A VOLUME
    Instance instance = findInstance(ec2, instanceId);
    String zone = instance.getPlacement().getAvailabilityZone();
    CreateVolumeRequest newVolumeRequest = new CreateVolumeRequest();
    newVolumeRequest.setSize(1); //1.0GB
    newVolumeRequest.setAvailabilityZone(zone);// set its available zone, it may change.

    CreateVolumeResult volumeResult = ec2.createVolume(newVolumeRequest);

    Volume v1 = volumeResult.getVolume();
    String volumeID = v1.getVolumeId();
    AttachVolumeRequest avr = new AttachVolumeRequest();//begin to attach the volume to instance
    avr.withInstanceId(instanceId);
    avr.withVolumeId(volumeID);
    avr.withDevice(mountPoint); //mount it
    ec2.attachVolume(avr);
    System.out.println("EBS volume has been attached and the volume ID is: " + volumeID);
    return (volumeID);
  }

  /**
   * Find an instance
   *
   * @param ec2
   * @param createdInstanceId
   * @return the name of the instance or null
   */
  public static Instance findInstance(AmazonEC2 ec2, String createdInstanceId) {
    DescribeInstancesResult describeInstancesResult = ec2.describeInstances();
    List<Reservation> reservations = describeInstancesResult.getReservations();
    Set<Instance> instances = new HashSet<>();
    // add all instances to a Set.
    for (Reservation reservation : reservations) {
      instances.addAll(reservation.getInstances());
    }
    for (Instance instance : instances) {
      if (createdInstanceId.equals(instance.getInstanceId())) {
        return instance;
      }
    }
    return null;
  }

  /**
   * Returns all the instances in this region.
   *
   * @param ec2
   * @return a set of instance instances
   */
  public static Set<Instance> getInstances(AmazonEC2 ec2) {
    Set<Instance> instances = new HashSet<>();
    DescribeInstancesResult describeInstancesResult = ec2.describeInstances();
    List<Reservation> reservations = describeInstancesResult.getReservations();

    // add all instances to a Set.
    for (Reservation reservation : reservations) {
      instances.addAll(reservation.getInstances());
    }
    return instances;
  }

  /**
   * Print public DNS and IP of an instance
   *
   * @param ec2
   * @param createdInstanceId
   */
  public static void describeInstanceDNSAndIP(AmazonEC2 ec2, String createdInstanceId) {
    Instance instance = findInstance(ec2, createdInstanceId);
    if (instance != null) {
      StringBuilder output = new StringBuilder();
      output.append("The public DNS is: ").append(instance.getPublicDnsName());
      output.append(NEWLINE);
      output.append("The private IP is: ").append(instance.getPrivateIpAddress());
      output.append(NEWLINE);
      output.append("The public IP is: ").append(instance.getPublicIpAddress());
      GNSConfig.getLogger().info(output.toString());
    }
  }

  /**
   * Adds the key and value as a 'tag' for the instance.
   *
   * @param ec2
   * @param createdInstanceId
   * @param key
   * @param value
   */
  public static void addInstanceTag(AmazonEC2 ec2, String createdInstanceId, String key, String value) {
    List<String> resources = new LinkedList<>();
    resources.add(createdInstanceId);

    List<Tag> tags = new LinkedList<>();
    Tag nameTag = new Tag(key, value);
    tags.add(nameTag);

    CreateTagsRequest ctr = new CreateTagsRequest(resources, tags);
    ec2.createTags(ctr);
  }

  /**
   * Adds keys and values from tagmap to the tags of the given instance.
   *
   * @param ec2
   * @param createdInstanceId
   * @param tagmap
   */
  public static void addInstanceTags(AmazonEC2 ec2, String createdInstanceId, Map<String, String> tagmap) {
    List<String> resources = new LinkedList<>();
    resources.add(createdInstanceId);

    List<Tag> tags = new LinkedList<>();
    for (Entry<String, String> entry : tagmap.entrySet()) {
      Tag nameTag = new Tag(entry.getKey(), entry.getValue());
      tags.add(nameTag);
    }
    CreateTagsRequest ctr = new CreateTagsRequest(resources, tags);
    ec2.createTags(ctr);
  }

  /**
   * Stop an instance
   *
   * @param ec2
   * @param createdInstanceId
   */
  public static void stopInstance(AmazonEC2 ec2, String createdInstanceId) {
    System.out.println("Stopping Instance:" + createdInstanceId);
    List<String> instanceIds = new LinkedList<>();
    instanceIds.add(createdInstanceId);

    StopInstancesRequest stopIR = new StopInstancesRequest(instanceIds);
    ec2.stopInstances(stopIR);
  }

  /**
   * Start an instance
   *
   * @param ec2
   * @param createdInstanceId
   */
  public static void startInstance(AmazonEC2 ec2, String createdInstanceId) {
    System.out.println("Starting Instance:" + createdInstanceId);
    List<String> instanceIds = new LinkedList<>();
    instanceIds.add(createdInstanceId);
    StartInstancesRequest startIR = new StartInstancesRequest(instanceIds);
    ec2.startInstances(startIR);
  }

  /**
   * Terminate an instance
   *
   * @param ec2
   * @param createdInstanceId
   */
  public static void terminateInstance(AmazonEC2 ec2, String createdInstanceId) {
    System.out.println("Terminating Instance:" + createdInstanceId);
    List<String> instanceIds = new LinkedList<>();
    instanceIds.add(createdInstanceId);
    TerminateInstancesRequest tir = new TerminateInstancesRequest(instanceIds);
    ec2.terminateInstances(tir);
  }

  /**
   *
   * @param ec2
   */
  public static void describeAllEndpoints(AmazonEC2 ec2) {
    for (RegionRecord endpoint : RegionRecord.values()) {
      System.out.println("*** " + endpoint.getURL() + " (" + endpoint.getLocation() + ") ***");
      currentTab = "  ";
      setRegion(ec2, endpoint);
      describeAvailabilityZones(ec2);
      //describeImages(ec2);
      describeKeyPairs(ec2);
      describeSecurityGroups(ec2);
      describeElasticIPs(ec2);
      describeInstances(ec2);
    }
  }

  /**
   *
   */
  public static int DEFAULTREACHABILITYWAITTIME = 240000; // FOUR minutes...

  /**
   * Creates an EC2 instance in the region given.
   *
   * @param ec2
   * @param region
   * @param ami
   * @param instanceName
   * @param keyName
   * @param securityGroupName
   * @param script
   * @param tags
   * @param elasticIP - an IP string or null to indicate that we're just using the address assigned by AWS
   * @return a new instance instance
   */
  public static Instance createAndInitInstance(AmazonEC2 ec2, RegionRecord region, AMIRecord ami, String instanceName,
          String keyName, String securityGroupName, String script, Map<String, String> tags, String elasticIP) {
    return createAndInitInstance(ec2, region, ami, instanceName, keyName, securityGroupName, script, tags, elasticIP, DEFAULTREACHABILITYWAITTIME);
  }

  /**
   * Creates an EC2 instance in the region given. Timeout in milleseconds can be specified.
   *
   * @param ec2
   * @param region
   * @param amiRecord
   * @param instanceName
   * @param keyName
   * @param securityGroupName
   * @param script
   * @param tags
   * @param elasticIP
   * @param timeout
   * @return a new instance instance
   */
  public static Instance createAndInitInstance(AmazonEC2 ec2, RegionRecord region, AMIRecord amiRecord, String instanceName,
          String keyName, String securityGroupName, String script, Map<String, String> tags, String elasticIP, int timeout) {

    try {
      // set the region (AKA endpoint)
      setRegion(ec2, region);
      // create the instance
      SecurityGroup securityGroup = findOrCreateSecurityGroup(ec2, securityGroupName);
      String keyPair = findOrCreateKeyPair(ec2, keyName);
      String instanceID = createInstanceAndWait(ec2, amiRecord, keyPair, securityGroup);
      if (instanceID == null) {
        return null;
      }
      System.out.println("Instance " + instanceName + " is running in " + region.name());

      // add a name to the instance
      addInstanceTag(ec2, instanceID, "Name", instanceName);
      if (tags != null) {
        addInstanceTags(ec2, instanceID, tags);
      }
      Instance instance = findInstance(ec2, instanceID);
      if (instance == null) {
        return null;
      }
      String hostname = instance.getPublicDnsName();
      System.out.println("Waiting " + timeout / 1000 + " seconds for " + instanceName + " (" + hostname + ", " + instanceID + ") to be reachable.");
      long startTime = System.currentTimeMillis();
      while (!Pinger.isReachable(hostname, SSHPORT, 2000)) {
        ThreadUtils.sleep(1000);
        System.out.print(".");
        if (System.currentTimeMillis() - startTime > timeout) {
          System.out.println(instanceName + " (" + hostname + ")" + " timed out during reachability check.");
          return null;
        }
      }
      System.out.println();
      System.out.println(instanceName + " (" + hostname + ")" + " is reachable.");

      // associate the elasticIP if one is provided
      if (elasticIP != null) {
        System.out.println("Using ElasticIP " + elasticIP + " for instance " + instanceName + " (" + instanceID + ")");
        AWSEC2.associateAddress(ec2, elasticIP, instance);

        // get a new copy cuz things have changed
        instance = findInstance(ec2, instanceID);
        if (instance == null) {
          return null;
        }
        // recheck reachability
        hostname = instance.getPublicDnsName();
        System.out.println("Waiting " + timeout / 1000 + " s for " + instanceName + " (" + hostname + ", " + instanceID + ") to be reachable after Elastic IP change.");
        startTime = System.currentTimeMillis();
        while (!Pinger.isReachable(hostname, SSHPORT, 2000)) {
          ThreadUtils.sleep(1000);
          System.out.print(".");
          if (System.currentTimeMillis() - startTime > timeout) {// give it a minute and ahalf
            System.out.println(instanceName + " (" + hostname + ")" + " timed out during second (elastic IP) reachability check.");
            return null;
          }
        }
        System.out.println();
        System.out.println(instanceName + " (" + hostname + ")" + " is still reachable.");
      }
      if (script != null) {
        File keyFile = new File(KEYHOME + FILESEPARATOR + keyName + PRIVATEKEYFILEEXTENSION);
        ExecuteBash.executeBashScript("ec2-user", hostname, keyFile, true, "installScript.sh", script);
      }
      return instance;

    } catch (AmazonServiceException ase) {
      System.out.println("Caught Exception: " + ase.getMessage());
      System.out.println("Reponse Status Code: " + ase.getStatusCode());
      System.out.println("Error Code: " + ase.getErrorCode());
      System.out.println("Request ID: " + ase.getRequestId());
    }
    return null;
  }

  /**
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    AWSCredentials credentials = new PropertiesCredentials(
            AWSEC2.class.getResourceAsStream(System.getProperty("user.home")
                    + FILESEPARATOR + ".aws" + FILESEPARATOR + "credentials"));
    //Create Amazon Client object
    AmazonEC2 ec2 = new AmazonEC2Client(credentials);

    RegionRecord region = RegionRecord.US_EAST_1;
    String keyName = "aws";
    String installScript = "#!/bin/bash\n"
            + "cd /home/ec2-user\n"
            + "yum --quiet --assumeyes update\n";
    HashMap<String, String> tags = new HashMap<>();
    tags.put("runset", new Date().toString());

    createAndInitInstance(ec2, region, AMIRecord.getAMI(AMIRecordType.Amazon_Linux_AMI_2013_03_1, region),
            "Test Instance", keyName, DEFAULT_SECURITY_GROUP_NAME, installScript, tags, "23.21.120.250");
  }
}
