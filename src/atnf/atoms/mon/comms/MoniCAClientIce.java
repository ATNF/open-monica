//
// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.comms;

import java.math.BigInteger;
import java.util.Date;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import atnf.atoms.mon.Alarm;
import atnf.atoms.mon.PointData;
import atnf.atoms.mon.PointDescription;
import atnf.atoms.mon.SavedSetup;
import atnf.atoms.mon.util.RSA;
import atnf.atoms.time.AbsTime;
import atnf.atoms.time.RelTime;

import atnf.atoms.mon.comms.MoniCAIcePrx;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.InitializationData;
import com.zeroc.Ice.ObjectPrx;
import com.zeroc.Ice.OperationNotExistException;
import com.zeroc.IceStorm.*;

/**
 * Ice implementation of a MoniCAClient.
 *
 * @author David Brodrick, Simon Hoyle
 */
public class MoniCAClientIce extends MoniCAClient {
  /** Host name that we are connected to. */
  protected String itsHost;

  /** The port to connect to on the server. */
  protected int itsPort = getDefaultPort();

  /** The underlying Ice client. */
  protected MoniCAIcePrx itsIceClient;

  /** The Ice communicator used to talk with the server. */
  protected Communicator itsCommunicator;

  /** Ice properties used to create the Communicator. */
  protected com.zeroc.Ice.Properties itsProperties;

  /** The Ice encoding version to use. 1.0 for backwards compatibility. */
  protected final String theirIceEncoding = "1.0";

  /** Maximum number of point name strings to request in a single fetch. */
  protected int theirMaxNameReq = Integer.parseInt(System.getProperty("MoniCA.MaxNames", "3000"));

  /** Maximum number of point definitions to request in a single fetch. */
  protected int theirMaxPointsReq = Integer.parseInt(System.getProperty("MoniCA.MaxPoints", "1000"));

  /**
   * Connect using the specified properties to find the MoniCA server via a locator.
   */
  public MoniCAClientIce(com.zeroc.Ice.Properties props) throws Exception {
    itsProperties = props;
    // Ensure the encoding is set to the appropriate version
    itsProperties.setProperty("Ice.Default.EncodingVersion", theirIceEncoding);
    connect();
  }

  /** Connect to the specified host. */
  public MoniCAClientIce(String host) throws Exception {
    itsHost = host;
    connect();
  }

  /** Connect to the specified host and port. */
  public MoniCAClientIce(String host, int port) throws Exception {
    itsHost = host;
    itsPort = port;
    connect();
  }

  /**
   * Return the current connection status.
   *
   * @return Connection status, True if connected, False if disconnected.
   */
  public boolean isConnected() {
    if (itsIceClient == null) {
      return false;
    } else {
      return true;
    }
  }

   /**
   * Get the names of all points (including aliases) on the system.
   *
   * @return Names of all points on the system.
   */
  public Vector<String> getAllPointNames() throws Exception {
    String[] tempnames = null;
    Vector<String> names = new Vector<String>(theirMaxNameReq);
    try {
      if (!isConnected()) {
        connect();
      }
      try {
        int start = 0;
        while (true) {
          // Request the next chunk of the point names
          tempnames = itsIceClient.getAllPointNamesChunk(start, theirMaxNameReq);
          for (int i = 0; i < tempnames.length; i++) {
              names.add(tempnames[i]);
          }
          // System.err.println("MoniCAClientIce.getAllPointNames: " + tempnames.length + " " + res.length);
          if (tempnames.length < theirMaxNameReq) {
            // We have finished retrieving all of the names
            break;
          }
          start = start + theirMaxNameReq;
	  tempnames = null;
        }
      } catch (OperationNotExistException f) {
        // Fall back to old method (prone to exceeded max message size if too many points)
        System.out.println("MoniCAClientIce::getAllPointNames: " + f.toString());
        tempnames = itsIceClient.getAllPointNames();
        for (int i = 0; i < tempnames.length; i++) {
              names.add(tempnames[i]);
        }
	return names;
      }
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getAllPointNames:" + e.getClass());
      // e.printStackTrace();
      disconnect();
      throw e;
    }

    return names;
  }

  /**
   * Get the point with the specified name. The populateClientFields method should be invoked on each point prior to returning the
   * result.
   *
   * @param pointname
   *          Point names to be retrieved.
   * @return The point definitions.
   */
  public PointDescription getPoint(String pointname) throws Exception {
    Vector<String> request = new Vector<String>(1);
    request.add(pointname);
    Vector<PointDescription> res = getPoints(request);
    if (res != null && res.size() > 0)
      return res.get(0);
    else
      return null;
  }

  /**
   * Get the points with the specified names. The populateClientFields method should be invoked on each point prior to returning the
   * result.
   *
   * @param pointnames
   *          Vector containing point names to be retrieved.
   * @return Vector containing all point definitions.
   */
  public Vector<PointDescription> getPoints(Vector<String> pointnames) throws Exception {
    Vector<PointDescription> res = null;

    // Recursively split large requests
    if (pointnames.size() > theirMaxPointsReq) {
      res = new Vector<PointDescription>(pointnames.size());
      int firstpoint = 0;
      while (firstpoint < pointnames.size()) {
        Vector<String> thesenames = new Vector<String>(pointnames.subList(firstpoint, Math.min(firstpoint + theirMaxPointsReq, pointnames.size())));
        // System.err.println("MoniCAClientIce.getPoints: Retrieving to " + firstpoint + " - " + (firstpoint +
        // thesenames.size()-1));
        res.addAll(getPoints(thesenames));
        firstpoint += theirMaxPointsReq;
      }
      return res;
    }

    try {
      if (!isConnected()) {
        connect();
      }
      // Pack names into array
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }
      // Ask the server for the PointDescription data
      PointDescriptionIce[] icepoints = itsIceClient.getPoints(namesarray);
      if (icepoints.length == 0) {
        // None of the requested points exist
        icepoints = new PointDescriptionIce[pointnames.size()];
        for (int i = 0; i < pointnames.size(); i++) {
          icepoints[i] = null;
        }
      } else if (icepoints.length != namesarray.length) {
        // Some points were not found so pack null elements in those result locations.
        PointDescriptionIce[] icepointstemp = new PointDescriptionIce[pointnames.size()];
        int nextpoint = 0;
        for (int i = 0; i < pointnames.size() && nextpoint < icepoints.length; i++) {
          String reqname = pointnames.get(i);
          for (String icename : icepoints[nextpoint].names) {
            if (reqname.equals(icepoints[nextpoint].source + "." + icename)) {
              // Found the Ice result for this requested point
              icepointstemp[i] = icepoints[nextpoint];
              nextpoint++;
              break;
            }
          }
        }
        icepoints = icepointstemp;
      }
      // Convert Ice packaging to PointDescription objects
      res = MoniCAIceUtil.getPointDescriptionsFromIce(icepoints);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getPoints:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Get all of the points on the system.
   *
   * @return Vector containing all point definitions.
   */
  public Vector<PointDescription> getAllPoints() throws Exception {
    Vector<PointDescription> res = new Vector<PointDescription>(theirMaxPointsReq, theirMaxPointsReq);

    try {
      if (!isConnected()) {
        connect();
      }
      try {
        int start = 0;
        while (true) {
          // Request the next chunk of the point names
          PointDescriptionIce[] icepoints = itsIceClient.getAllPointsChunk(start, theirMaxPointsReq);

          // Convert and append
          Vector<PointDescription> temppoints = MoniCAIceUtil.getPointDescriptionsFromIce(icepoints);
          res.addAll(temppoints);

          // System.err.println("MonClientIce.getAllPoints " + temppoints.size() + " " + res.size());

          if (temppoints.size() < theirMaxPointsReq) {
            // We have finished retrieving all of the names
            break;
          }

          start = start + theirMaxPointsReq;
        }
      } catch (OperationNotExistException f) {
        // Fall back to old method (prone to exceeded max message size if too many points)
        PointDescriptionIce[] icepoints = itsIceClient.getAllPoints();
        res = MoniCAIceUtil.getPointDescriptionsFromIce(icepoints);
      }
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getAllPoints:" + e.getClass());
      // e.printStackTrace();
      disconnect();
      throw e;
    }

    return res;
  }

  /**
   * Add multiple points to the servers. This is a privileged operation which requires the user to authenticate against the server.
   * The username and password are encrypted prior to transmission over the network.
   *
   * @param newpoints
   *          Definitions for the new points.
   * @param username
   *          Username to authenticate against server.
   * @param passwd
   *          Password to authenticate against server.
   * @return True if points added, False if not added.
   */
  public boolean addPoints(Vector<PointDescription> newpoints, String username, String passwd) throws Exception {
    boolean res = false;
    try {
      if (!isConnected()) {
        connect();
      }
      PointDescriptionIce[] icepoints = MoniCAIceUtil.getPointDescriptionsAsIce(newpoints);
      // Encrypt the username/password
      RSA encryptor = getEncryptor();
      String encname = encryptor.encrypt(username);
      String encpass = encryptor.encrypt(passwd);
      res = itsIceClient.addPoints(icepoints, encname, encpass);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.addPoints:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Return the latest data for all of the named points.
   *
   * @param pointnames
   *          Points to obtain data for.
   * @return Vector of latest values in same order as argument.
   */
  public Vector<PointData> getData(Vector<String> pointnames) throws Exception {
    Vector<PointData> res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }
      PointDataIce[] icedata = itsIceClient.getData(namesarray);
      res = MoniCAIceUtil.getPointDataFromIce(icedata);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getData:" + e);
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Return the last data before the specified timestamp.
   *
   * @param pointnames
   *          Points to obtain data for.
   * @param t
   *          The reference timestamp.
   * @return Vector of last values in same order as argument.
   */
  public Vector<PointData> getBefore(Vector<String> pointnames, AbsTime t) throws Exception {
    Vector<PointData> res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }
      PointDataIce[] icedata = itsIceClient.getBefore(namesarray, t.getValue());
      res = MoniCAIceUtil.getPointDataFromIce(icedata);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getBefore:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Return the next data after the specified timestamp.
   *
   * @param pointnames
   *          Points to obtain data for.
   * @param t
   *          The reference timestamp.
   * @return Vector of next values in same order as argument.
   */
  public Vector<PointData> getAfter(Vector<String> pointnames, AbsTime t) throws Exception {
    Vector<PointData> res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }
      PointDataIce[] icedata = itsIceClient.getAfter(namesarray, t.getValue());
      res = MoniCAIceUtil.getPointDataFromIce(icedata);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getAfter:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Return archived data for the given points.
   *
   * @param pointnames
   *          Names of points to get data for.
   * @param start
   *          The oldest data to be retrieved.
   * @param end
   *          The most recent data to be retrieved.
   * @param maxsamples
   *          Maximum number of records to be returned.
   * @return Data from the archive between the specified times, for each point.
   */
  public Vector<Vector<PointData>> getArchiveData(Vector<String> pointnames, AbsTime start, AbsTime end, int maxsamples) throws Exception {
    Vector<Vector<PointData>> res = new Vector<Vector<PointData>>(pointnames.size());
    try {
      if (!isConnected()) {
        connect();
      }
      // Get data for each point in turn, as server may only return part of the
      // data each time, so we need to iterate until all data has been retrieved
      for (int thispoint = 0; thispoint < pointnames.size(); thispoint++) {
        String thisname = pointnames.get(thispoint);
        Vector<PointData> thisdata = new Vector<PointData>();
        AbsTime thisstart = start;
        while (true) {
          PointDataIce[][] icedata = itsIceClient.getArchiveData(new String[] { thisname }, thisstart.getValue(), end.getValue(), maxsamples);
          if (icedata != null && icedata.length > 0 && icedata[0].length > 0) {
            // Convert data to native representation
            Vector<PointData> newdata = MoniCAIceUtil.getPointDataFromIce(icedata[0]);
            // Reinsert name fields dropped by server to minimise bandwidth
            for (int j = 0; j < newdata.size(); j++) {
              newdata.get(j).setName(thisname);
            }
            thisdata.addAll(MoniCAIceUtil.getPointDataFromIce(icedata[0]));
            if (icedata[0].length == 1) {
              // No data will be returned to subsequent queries so stop now
              break;
            }
          } else {
            // System.err.println("MoniCAClientIce.getArchiveData: " + pointnames.get(thispoint) + ": No New Data");
            // No new data was returned
            break;
          }
          thisstart = ((PointData) (thisdata.get(thisdata.size() - 1))).getTimestamp().add(RelTime.factory(1));
        }
        res.add(thisdata);
      }
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getArchiveData:" + e);
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Set new values for the specified points. This requires authentication. The username and password are encrypted prior to
   * transmission over the network.
   *
   * @param pointnames
   *          Names of the points to set values for.
   * @param values
   *          New values to be assigned to the points.
   * @param username
   *          Username to authenticate.
   * @param passwd
   *          Password to authenticate the user.
   * @return The latest data available on the server.
   */
  public boolean setData(Vector<String> pointnames, Vector<PointData> values, String username, String passwd) throws Exception {
    boolean res = false;
    try {
      if (!isConnected()) {
        connect();
      }
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }
      PointDataIce[] icevalues = MoniCAIceUtil.getPointDataAsIce(values);
      // Encrypt the username/password
      RSA encryptor = getEncryptor();
      String encname = encryptor.encrypt(username);
      String encpass = encryptor.encrypt(passwd);
      res = itsIceClient.setData(namesarray, icevalues, encname, encpass);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.setData:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Return all SavedSetups for client Objects from the server. <tt>null</tt> may be returned if the server has no SavedSetups.
   */
  public Vector<SavedSetup> getAllSetups() throws Exception {
    Vector<SavedSetup> res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      // Get the setup strings from server and construct SavedSetup objects from them
      String[] setupstrings = itsIceClient.getAllSetups();
      if (setupstrings != null && setupstrings.length > 0) {
        res = new Vector<SavedSetup>(setupstrings.length);
        for (int i = 0; i < setupstrings.length; i++) {
          SavedSetup thissetup = new SavedSetup(setupstrings[i]);
          if (thissetup != null) {
            res.add(thissetup);
          }
        }
      }
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getAllSetups:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Add a new SavedSetup to the server. This requires authentication to prevent inappropriate modification of the server data. The
   * username and password are encrypted prior to transmission over the network.
   *
   * @param setup
   *          The SavedSetup to add to the server.
   * @param username
   *          Username to authenticate.
   * @param passwd
   *          Password to authenticate the user.
   * @return True if the setup was added, False if it couldn't be added.
   */
  public boolean addSetup(SavedSetup setup, String username, String passwd) throws Exception {
    boolean res = false;
    try {
      if (!isConnected()) {
        connect();
      }
      String stringsetup = setup.toString();
      // Encrypt the username/password
      RSA encryptor = getEncryptor();
      String encname = encryptor.encrypt(username);
      String encpass = encryptor.encrypt(passwd);
      res = itsIceClient.addSetup(stringsetup, encname, encpass);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.addSetup:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Get all alarm points with priority >=0, whether they are alarming or not.
   *
   * @return List of alarms or null if none are defined.
   */
  public Vector<Alarm> getAllAlarms() throws Exception {
    Vector<Alarm> res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      // Get the alarms from the server and construct local representations from them
      AlarmIce[] icealarms = itsIceClient.getAllAlarms();
      if (icealarms != null && icealarms.length > 0) {
        // First check if we need to fetch any new point definitions
        Vector<String> fetchpoints = null;
        for (int i = 0; i < icealarms.length; i++) {
          String pointname = icealarms[i].pointname;
          if (PointDescription.getPoint(pointname) == null) {
            // We need to fetch the point definition for this point
            // System.err.println("MoniCAClientIce.getAllAlarms: Fetching PointDescription for " + pointname);
            if (fetchpoints == null) {
              fetchpoints = new Vector<String>(icealarms.length);
            }
            fetchpoints.add(pointname);
          }
        }
        if (fetchpoints != null) {
          getPoints(fetchpoints);
        }

        res = new Vector<Alarm>(icealarms.length);
        for (int i = 0; i < icealarms.length; i++) {
          Alarm thisalarm = MoniCAIceUtil.getAlarmFromIce(icealarms[i]);
          if (thisalarm != null) {
            res.add(thisalarm);
          }
        }
      }
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getAllAlarms:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Get all points which are currently alarming (including acknowledged) or shelved.
   *
   * @return List of alarms or null if none meet the criteria.
   */
  public Vector<Alarm> getCurrentAlarms() throws Exception {
    Vector<Alarm> res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      // Get the alarms from the server and construct local representations from them
      AlarmIce[] icealarms = itsIceClient.getCurrentAlarms();
      if (icealarms != null && icealarms.length > 0) {
        res = new Vector<Alarm>(icealarms.length);
        for (int i = 0; i < icealarms.length; i++) {
          String pointname = icealarms[i].pointname;
          if (PointDescription.getPoint(pointname) == null) {
            // We need to fetch the point definition for this point
            System.err.println("MoniCAClientIce.getCurrentAlarms: Fetching PointDescription for " + pointname);
            getPoint(pointname);
          }
          Alarm thisalarm = MoniCAIceUtil.getAlarmFromIce(icealarms[i]);
          if (thisalarm != null) {
            res.add(thisalarm);
          }
        }
      }
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getCurrentAlarms:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Acknowledge/deacknowledge the specified alarm points.
   *
   * @param pointnames
   *          The points to be acknowledged or deacknowledged.
   * @param ack
   *          True to acknowledge an alarm or false to deacknowledge.
   * @param username
   *          Name of the user performing the operation.
   * @param password
   *          Password hash for the user performing the operation.
   * @return False if permission was denied for one or more points.
   */
  public boolean acknowledgeAlarms(Vector<String> pointnames, boolean ack, String username, String password) throws Exception {
    boolean res = false;
    try {
      // Format request into array
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }
      // Encrypt the username/password
      RSA encryptor = getEncryptor();
      String encname = encryptor.encrypt(username);
      String encpass = encryptor.encrypt(password);
      // Send the request to the server
      res = itsIceClient.acknowledgeAlarms(namesarray, ack, encname, encpass);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.acknowledgeAlarms:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Shelve/deshelved the specified alarm points.
   *
   * @param pointnames
   *          The points to be shelved or deshelved.
   * @param shelve
   *          True to shelve an alarm or false to deshelve.
   * @param username
   *          Name of the user performing the operation.
   * @param password
   *          Password hash for the user performing the operation.
   * @return False if permission was denied for one or more points.
   */
  public boolean shelveAlarms(Vector<String> pointnames, boolean shelve, String username, String password) throws Exception {
    boolean res = false;
    try {
      // Format request into array
      String[] namesarray = new String[pointnames.size()];
      for (int i = 0; i < pointnames.size(); i++) {
        namesarray[i] = pointnames.get(i);
      }

      // Encrypt the username/password
      RSA encryptor = getEncryptor();
      String encname = encryptor.encrypt(username);
      String encpass = encryptor.encrypt(password);

      // Send the request to the server
      res = itsIceClient.shelveAlarms(namesarray, shelve, encname, encpass);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.shelveAlarms:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /**
   * Return an RSA encryptor that uses the servers public key and modulus. This will allow us to encrypt information that can only
   * be encrypted by the server.
   */
  public RSA getEncryptor() throws Exception {
    RSA res = null;
    try {
      if (!isConnected()) {
        connect();
      }
      // Get the public key and modulus
      String[] a = itsIceClient.getEncryptionInfo();
      BigInteger e = new BigInteger(a[0]);
      BigInteger n = new BigInteger(a[1]);
      res = new RSA(n, e);
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getEncryptor:" + e.getClass());
      disconnect();
      throw e;
    }
    return res;
  }

  /** Get the current time on the server. */
  public AbsTime getCurrentTime() throws Exception {
    try {
      if (!isConnected()) {
        connect();
      }

      long servertime = itsIceClient.getCurrentTime();
      return AbsTime.factory(servertime);

    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getCurrentTime:" + e.getClass());
      disconnect();
      throw e;
    }
  }

  /** Get the epoch/leap seconds dictionary from the server. */
  public SortedMap<Date, Integer> getLeapSeconds() throws Exception {
    try {
      if (!isConnected()) {
        connect();
      }

      dUTCEntry[] leapseconds = itsIceClient.getLeapSeconds();
      SortedMap<Date, Integer> res = new TreeMap<Date, Integer>();

      for (int i = 0; i < leapseconds.length; i++) {
        res.put(new Date(leapseconds[i].epoch), new Integer(leapseconds[i].dUTC));
      }

      return res;
    } catch (Exception e) {
      System.err.println("MoniCAClientIce.getLeapSeconds:" + e.getClass());
      disconnect();
      throw e;
    }
  }

  protected boolean connect() throws Exception {
    try {
      itsCommunicator = null;
      ObjectPrx base = null;
      if (itsProperties == null) {
        // Connect directly to the specified server
        itsCommunicator = com.zeroc.Ice.Util.initialize();
        // Use encoding 1.0 for backwards compatibility
        base = itsCommunicator.stringToProxy("MoniCAService -e " + theirIceEncoding + ": tcp -h " + itsHost + " -p " + itsPort + " -t 30000");
      } else {
        // Find the server via a Locator service
        InitializationData id = new InitializationData();
        id.properties = itsProperties;
        itsCommunicator = com.zeroc.Ice.Util.initialize(id);
        String adaptername = itsProperties.getProperty("AdapterName");
        if (adaptername == null) {
          base = itsCommunicator.stringToProxy("MoniCAService");
        } else {
          base = itsCommunicator.stringToProxy("MoniCAService@" + adaptername);
        }
      }
      itsIceClient = MoniCAIcePrx.checkedCast(base);
      if (itsIceClient == null) {
        throw new Error("MoniCAClientIce.connect: Invalid proxy");
      }
    } catch (Exception e) {
      disconnect();
      throw e;
    }
    return true;
  }

  /** Disconnect from the server. */
  protected void disconnect() {
    if (itsCommunicator != null) {
      try {
        itsCommunicator.destroy();
      } catch (Exception e) {
        System.err.println("MoniCAClientIce.disconnect: " + e.getClass());
      }
    }
    itsCommunicator = null;
    itsIceClient = null;
  }

  /** Get the default port for client server communication. */
  public static int getDefaultPort() {
    return MoniCAIceUtil.getDefaultPort();
  }
}
