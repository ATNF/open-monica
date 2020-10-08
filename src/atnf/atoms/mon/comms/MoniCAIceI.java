// Copyright (c) 2003-2008 ZeroC, Inc. All rights reserved.
// Copyright (C) 2009 CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.comms;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.Vector;
import java.util.Date;

import atnf.atoms.mon.*;
import atnf.atoms.mon.util.MonitorConfig;
import atnf.atoms.mon.util.RADIUSAuthenticator;
import atnf.atoms.time.*;
import org.apache.log4j.Logger;

import atnf.atoms.mon.comms.MoniCAIce;
import com.zeroc.Ice.Current;
import com.zeroc.Ice.ObjectAdapter;
import com.zeroc.Ice.Communicator;
import com.zeroc.Ice.InitializationData;


/**
 * Concrete implementation of the Ice server for MoniCA.
 *
 * @author David Brodrick
 */
public final class MoniCAIceI implements MoniCAIce {
  /** The currently running server. */
  protected static MoniCAIceServerThread theirServer = null;

  protected static Logger theirLogger = Logger.getLogger(MoniCAIceI.class.getName());

  /** Port the server will listen on. */
  protected static int theirPort;

  static {
    // Determine which port the server should listen on
    try {
      theirPort = Integer.parseInt(MonitorConfig.getProperty("IcePort", "" + MoniCAIceUtil.getDefaultPort()));
    } catch (Exception e) {
      theirLogger.fatal("Error parsing IcePort configuration parameter: " + e);
      System.exit(1);
    }
  }

  public MoniCAIceI() {
  }

  /** Add the new points to the system. */
  public boolean addPoints(PointDescriptionIce[] newpoints, String encname, String encpass, Current __current) {
    // Check user's credentials
    String authuser = checkAuth(encname, encpass, getRemoteHost(__current));
    if (authuser == null) {
      theirLogger.warn("addPoints: Failed authentication attempt from " + getRemoteInfo(__current));
      return false;
    }

    // TODO: Currently does nothing
    theirLogger.warn("addPoints method called by " + authuser + " but is currently unimplemented");
    return false;
  }

  /** Get the names of all points on the system (including aliases). */
  public String[] getAllPointNames(Current __current) {
    return PointDescription.getAllPointNames();
  }

  /** Get the requested range of names of all points on the system (including aliases). */
  public String[] getAllPointNamesChunk(int start, int num, Current __current) {
    String[] allnames = PointDescription.getAllPointNames();
    String[] res;
    if (start < 0 || start >= allnames.length) {
      // Bad request, return zero-length array
      res = new String[0];
    } else {
      int reslen = Math.min(num, allnames.length - start);
      res = new String[reslen];
      for (int i = 0; i < reslen; i++) {
        res[i] = allnames[start + i];
      }
    }
    return res;
  }

  /** Get all unique points on the system. */
  public PointDescriptionIce[] getAllPoints(Current __current) {
    // Get all unique points
    PointDescription[] points = PointDescription.getAllUniquePoints();
    return MoniCAIceUtil.getPointDescriptionsAsIce(points);
  }

  /** Get the requested range of all unique points on the system. */
  public PointDescriptionIce[] getAllPointsChunk(int start, int num, Current __current) {
    PointDescriptionIce[] res;
    PointDescription[] allpoints = PointDescription.getAllUniquePoints();

    if (start < 0 || start >= allpoints.length) {
      // Bad request, return zero-length array
      res = new PointDescriptionIce[0];
    } else {
      int reslen = Math.min(num, allpoints.length - start);
      PointDescription[] temppoints = new PointDescription[reslen];
      for (int i = 0; i < reslen; i++) {
        temppoints[i] = allpoints[start + i];
      }
      res = MoniCAIceUtil.getPointDescriptionsAsIce(temppoints);
    }
    return res;
  }

  /** Return the definitions for the specified points. */
  public PointDescriptionIce[] getPoints(String[] names, Current __current) {
    PointDescriptionIce[] temp = new PointDescriptionIce[names.length];
    int numfound = 0;
    for (int i = 0; i < names.length; i++) {
      checkPoint(names[i], __current);
      PointDescription thispoint = PointDescription.getPoint(names[i]);
      if (thispoint != null) {
        temp[i] = MoniCAIceUtil.getPointDescriptionAsIce(thispoint);
        numfound++;
      }
    }
    PointDescriptionIce[] res;
    if (numfound == names.length) {
      res = temp;
    } else {
      res = new PointDescriptionIce[numfound];
      int nextres = 0;
      for (int i = 0; i < names.length; i++) {
        if (temp[i] != null) {
          res[nextres] = temp[i];
          nextres++;
        }
      }
    }
    return res;
  }

  /** Add the new setup to the system. */
  public boolean addSetup(String setup, String encname, String encpass, Current __current) {
    // Check user's credentials
    String authuser = checkAuth(encname, encpass, getRemoteHost(__current));
    if (authuser == null) {
      theirLogger.warn("addSetup: Failed authentication attempt from " + getRemoteInfo(__current));
      return false;
    }

    // TODO: Currently does nothing
    theirLogger.warn("addSetup method called by " + authuser + " but is currently unimplemented");
    return false;
  }

  /** Return the string representation of all saved setups stored on the server. */
  public String[] getAllSetups(Current __current) {
    SavedSetup[] allsetups = SavedSetup.getAllSetups();
    if (allsetups == null) {
      allsetups = new SavedSetup[0];
    }
    String[] stringsetups = new String[allsetups.length];
    for (int i = 0; i < allsetups.length; i++) {
      stringsetups[i] = allsetups[i].toString();
    }
    return stringsetups;
  }

  /** Return historical data for the specified points. */
  public PointDataIce[][] getArchiveData(String[] names, long start, long end, long maxsamples, Current __current) {
    AbsTime absstart = AbsTime.factory(start);
    AbsTime absend = AbsTime.factory(end);
    PointDataIce[][] res = new PointDataIce[names.length][];
    for (int i = 0; i < names.length; i++) {
      checkPoint(names[i], __current);
      // Get the requested data from the buffer/archive
      Vector<PointData> thisdata = PointBuffer.getPointData(names[i], absstart, absend, (int) maxsamples);
      if (thisdata == null) {
        // Ice doesn't like null, so replace with empty return structure
        thisdata = new Vector<PointData>(0);
	theirLogger.debug("getArchiveData: No archive data found for point " + names[i]);
      }
      else {
        theirLogger.debug("getArchiveData: Found " + thisdata.size() + " entries for point " + names[i]);
      }
      // Convert to Ice representation
      res[i] = MoniCAIceUtil.getPointDataAsIce(thisdata);
      // Blank the name to minimise network traffic
      for (int j = 0; j < res[i].length; j++) {
        res[i][j].name = "";
      }

    }
    return res;
  }

  /** Return the latest values for the given points. */
  public PointDataIce[] getData(String[] names, Current __current) {
    PointDataIce[] temp = new PointDataIce[names.length];
    for (int i = 0; i < names.length; i++) {
      checkPoint(names[i], __current);
      PointData pd = PointBuffer.getPointData(names[i]);
      PointDataIce pdi;
      if (pd != null) {
        pdi = MoniCAIceUtil.getPointDataAsIce(pd);
      } else {
        // No data available so create dummy data with null value
        pdi = MoniCAIceUtil.getPointDataAsIce(new PointData(names[i]));
      }
      temp[i] = pdi;
    }
    return temp;
  }

  /** Return the last values before the given time for the given points. */
  public PointDataIce[] getBefore(String[] names, long t, Current __current) {
    PointDataIce[] temp = new PointDataIce[names.length];
    for (int i = 0; i < names.length; i++) {
      checkPoint(names[i], __current);
      PointData pd = PointBuffer.getPreceding(names[i], AbsTime.factory(t));
      PointDataIce pdi;
      if (pd != null) {
        pdi = MoniCAIceUtil.getPointDataAsIce(pd);
      } else {
        // No data available so create dummy data with null value
        pdi = MoniCAIceUtil.getPointDataAsIce(new PointData(names[i]));
      }
      temp[i] = pdi;
    }
    return temp;
  }

  /** Return the next values after the given time for the given points. */
  public PointDataIce[] getAfter(String[] names, long t, Current __current) {
    PointDataIce[] temp = new PointDataIce[names.length];
    for (int i = 0; i < names.length; i++) {
      checkPoint(names[i], __current);
      PointData pd = PointBuffer.getFollowing(names[i], AbsTime.factory(t));
      PointDataIce pdi;
      if (pd != null) {
        pdi = MoniCAIceUtil.getPointDataAsIce(pd);
      } else {
        // No data available so create dummy data with null value
        pdi = MoniCAIceUtil.getPointDataAsIce(new PointData(names[i]));
      }
      temp[i] = pdi;
    }
    return temp;
  }

  /** Set new values for the specified points. */
  public boolean setData(String[] names, PointDataIce[] rawvalues, String encname, String encpass, Current __current) {
    if (names.length != rawvalues.length) {
      return false;
    }
    int numpoints = names.length;
    String reminfo = getRemoteInfo(__current);

    // Check user's credentials
    String authuser = checkAuth(encname, encpass, getRemoteHost(__current));
    if (authuser == null) {
      theirLogger.warn("setData: Failed authentication attempt from " + reminfo);
      return false;
    }

    // Process each of the control requests consecutively
    boolean result = true;
    Vector<PointData> values = MoniCAIceUtil.getPointDataFromIce(rawvalues);
    for (int i = 0; i < numpoints; i++) {
      checkPoint(names[i], __current);
      try {
        // Get the specified point
        PointDescription thispoint = PointDescription.getPoint(names[i]);
        if (thispoint == null) {
          theirLogger.warn("In setData method: Point " + names[i] + " does not exist");
          result = false;
          continue;
        }
        // Act on the new data value
        PointData newval = values.get(i);
        theirLogger.trace("Assigning value " + newval + " for \"" + authuser + "@" + reminfo + "\"");
        // AbsTime start = AbsTime.factory();
        thispoint.firePointEvent(new PointEvent(this, newval, true));
        // theirLogger.debug("Call took " + Time.diff(AbsTime.factory(), start).toString(RelTime.Format.SECS_BAT));
      } catch (Exception e) {
        theirLogger.warn("In setData method, while processing " + names[i] + ": " + e);
        result = false;
      }
    }

    return result;
  }

  /** Get all priority alarms defined on the system irrespective of current state. */
  public AlarmIce[] getAllAlarms(Current __current) {
    Vector<Alarm> allalarms = AlarmManager.getAllAlarms();
    AlarmIce[] res = MoniCAIceUtil.getAlarmsAsIce(allalarms);
    return res;
  }

  /** Get all active (or acknowledged) and shelved alarms on the system. */
  public AlarmIce[] getCurrentAlarms(Current __current) {
    Vector<Alarm> allalarms = AlarmManager.getAlarms();
    AlarmIce[] res = MoniCAIceUtil.getAlarmsAsIce(allalarms);
    return res;
  }

  /** Acknowledge (ack=true) or deacknowledge (ack=false) the specified alarms. */
  public boolean acknowledgeAlarms(String[] names, boolean ack, String encname, String encpass, Current __current) {
    boolean res = true;
    // Check user's credentials
    String authuser = checkAuth(encname, encpass, getRemoteHost(__current));

    if (authuser == null) {
      theirLogger.warn("acknowledgeAlarms: Failed authentication attempt from " + getRemoteInfo(__current));
      res = false;
    } else {
      for (int i = 0; i < names.length; i++) {
        checkPoint(names[i], __current);
        PointDescription thispoint = PointDescription.getPoint(names[i]);
        if (thispoint != null) {
          AlarmManager.setAcknowledged(thispoint, ack, authuser);
          theirLogger.debug("Point \"" + names[i] + "\" acknowledged=" + ack + " by \"" + authuser + "@" + getRemoteInfo(__current) + "\"");
        } else {
          res = false;
        }
      }
    }
    return res;
  }

  /** Shelve (shelve=true) or deshelve (shelve=false) the specified alarms. */
  public boolean shelveAlarms(String[] names, boolean shelve, String encname, String encpass, Current __current) {
    boolean res = true;
    // Check user's credentials
    String authuser = checkAuth(encname, encpass, getRemoteHost(__current));

    if (authuser == null) {
      theirLogger.warn("shelveAlarms: Failed authentication attempt from " + getRemoteInfo(__current));
      res = false;
    } else {
      // User is authenticated, so shelve the alarms
      for (int i = 0; i < names.length; i++) {
        checkPoint(names[i], __current);
        PointDescription thispoint = PointDescription.getPoint(names[i]);
        if (thispoint != null) {
          AlarmManager.setShelved(thispoint, shelve, authuser);
          theirLogger.debug("Point \"" + names[i] + "\" shelved=" + shelve + " by \"" + authuser + "@" + getRemoteInfo(__current) + "\"");
        } else {
          res = false;
        }
      }
    }
    return res;
  };

  /** Return the key and modulus required to send encrypted data to the server. */
  public String[] getEncryptionInfo(Current __current) {
    String[] a = new String[2];
    a[0] = KeyKeeper.getExponent();
    a[1] = KeyKeeper.getModulus();
    return a;
  }

  /** Return the current time on the server. */
  public long getCurrentTime(Current __current) {
    return (new AbsTime()).getValue();
  }

  public dUTCEntry[] getLeapSeconds(Current __current) {
    SortedMap<Date, Integer> leapseconds = DUTC.getLeapSeconds();
    dUTCEntry[] res = new dUTCEntry[leapseconds.size()];
    Object[] dates = leapseconds.keySet().toArray();
    for (int i = 0; i < dates.length; i++) {
      res[i] = new dUTCEntry();
      res[i].epoch = ((Date) dates[i]).getTime();
      res[i].dUTC = leapseconds.get(dates[i]);
    }
    return res;
  }

  /** Return validated username if credentials are valid or else return null. */
  private String checkAuth(String encname, String encpass, String host) {
    String username = KeyKeeper.decrypt(encname);
    String password = KeyKeeper.decrypt(encpass);
    if (!RADIUSAuthenticator.authenticate(username, password, host)) {
      // Authentication failed, add a delay
      username = null;
      try {
        RelTime.factory(1000000).sleep();
      } catch (Exception e) {
      }
    }
    return username;
  }

  /** Check if the point is valid and log appropriate messages if it is not. */
  private void checkPoint(String name, Current __current) {
    int type = PointDescription.checkPointNameType(name);
    if (type < 0) {
      theirLogger.debug("Request for non-existent point \"" + name + "\" from " + getRemoteInfo(__current));
    } else if (type > 0) {
      theirLogger.debug("Request for alias name \"" + name + "\" from " + getRemoteInfo(__current));
    }
  }

  /** Get the remote clients IP. */
  private String getRemoteHost(Current __current) {
    String temp = __current.con.toString();
    temp = temp.substring(temp.indexOf('\n') + 1);
    temp = temp.substring(temp.indexOf('=') + 1);
    temp = temp.substring(0, temp.indexOf(':')).trim();
    return temp;
  }

  /** Get the remote clients IP/port. */
  private String getRemoteInfo(Current __current) {
    String temp = __current.con.toString();
    temp = temp.substring(temp.indexOf('\n') + 1);
    temp = temp.substring(temp.indexOf('=') + 1).trim();
    return temp;
  }

  /** Start the server on the default port. */
  public static void startIceServer() {
    startIceServer(theirPort);
  }

  /** Start the server using the specified adapter. */
  public static void startIceServer(ObjectAdapter a) {
    if (theirServer != null) {
      theirServer.shutdown();
    }
    theirServer = new MoniCAIceServerThreadAdapter(a);
    theirServer.start();
  }

  /** Start the server using the specified port number. */
  public static void startIceServer(int port) {
    if (theirServer != null) {
      theirServer.shutdown();
    }
    theirServer = new MoniCAIceServerThreadPort(port);
    theirServer.start();
  }

  /** Shutdown the currently running Ice server. */
  public static void stopIceServer() {
    theirServer.shutdown();
    theirServer = null;
  }

  /** Start a new thread to run the server using an existing adapter. */
  public abstract static class MoniCAIceServerThread extends Thread {
    /** The name of the service which is registered with Ice. */
    protected static String theirServiceName = "MoniCAService";

    /** The adapter to start the server with. */
    protected ObjectAdapter itsAdapter = null;

    /** Logger. */
    protected Logger itsLogger = Logger.getLogger(this.getClass().getName());

    public MoniCAIceServerThread() {
      super("MoniCAIceServer");
    }

    /** Start the server and block until it is registered. */
    public void start() {
      super.start();
      // Block until service is registered
      while (itsAdapter == null || itsAdapter.find(itsAdapter.getCommunicator().stringToIdentity(theirServiceName)) == null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
    }

    /** Stop the server and block until it is deregistered. */
    public void shutdown() {
      itsAdapter.remove(itsAdapter.getCommunicator().stringToIdentity(theirServiceName));
      // Block until service is unregistered
      while (itsAdapter.find(itsAdapter.getCommunicator().stringToIdentity(theirServiceName)) != null) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
        }
      }
    }
  };

  /** Start a new thread to run the server using an existing adapter. */
  public static class MoniCAIceServerThreadAdapter extends MoniCAIceServerThread {
    public MoniCAIceServerThreadAdapter(ObjectAdapter a) {
      super();
      itsAdapter = a;
    }

    public void run() {
      Communicator ic = null;
      try {
        ic = itsAdapter.getCommunicator();
        com.zeroc.Ice.Object object = new MoniCAIceI();
        itsAdapter.add(object, ic.stringToIdentity(theirServiceName));
        ic.waitForShutdown();
      } catch (Exception e) {
        theirLogger.error("In run method: " + e);
      }
      if (ic != null) {
        try {
          ic.destroy();
        } catch (Exception e) {
          itsLogger.error("While destroying communicator: " + e);
        }
      }
    }
  };

  /** Start a new thread to run the server using a new adapter on specified port. */
  public static class MoniCAIceServerThreadPort extends MoniCAIceServerThread {
    /**
     * The port to start the server on. Not used if a adapter has been explicitly specified.
     */
    protected int itsPort;

    public MoniCAIceServerThreadPort(int port) {
      super();
      itsPort = port;
    }

    /** Stop the server and block until it is deregistered. */
    public void shutdown() {
      super.shutdown();
      itsAdapter.deactivate();
    }

    public void run() {
      Communicator ic = null;
      try {
        // Need to create a new adapter
        com.zeroc.Ice.Properties props = com.zeroc.Ice.Util.createProperties();
        props.setProperty("Ice.IPv6", "0");
        InitializationData id = new InitializationData();
        id.properties = props;
        ic = com.zeroc.Ice.Util.initialize(id);
        itsAdapter = ic.createObjectAdapterWithEndpoints("MoniCAIceAdapter", "tcp -p " + itsPort);
        com.zeroc.Ice.Object object = new MoniCAIceI();
        itsAdapter.add(object, ic.stringToIdentity(theirServiceName));
        itsAdapter.activate();
        ic.waitForShutdown();
      } catch (Exception e) {
        itsLogger.error("In run method: " + e);
      }
      if (ic != null) {
        try {
          ic.destroy();
        } catch (Exception e) {
          itsLogger.error("While destroying communicator: " + e);
        }
      }
    }
  }
}
