// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.externalsystem;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.Integer;
import java.util.concurrent.atomic.AtomicInteger;

import atnf.atoms.time.*;
import atnf.atoms.mon.*;
import atnf.atoms.mon.transaction.*;
import atnf.atoms.util.EnumItem;

import gov.aps.jca.*;
import gov.aps.jca.dbr.*;
import gov.aps.jca.event.*;

import com.cosylab.epics.caj.*;

import org.apache.log4j.Logger;

/**
 * Interface between MoniCA and EPICS Channel Access, allowing CA monitors (publish/subscribe) updates and gets (polling).
 * 
 * <P>
 * Channel access monitors can be established by using a <tt>TransactionEPICSMonitor</tt> as an input for the MoniCA point. The
 * transaction requires one argument which is the name of the EPICS Process Variable to be monitored.
 * 
 * <P>
 * Polling is implemented by using a <tt>TransactionEPICS</tt> as an input transaction. This requires the name of the EPICS Process
 * Variable to be polled as an argument and also the DBRType if the Transaction is being used for control/write operations. Polling
 * will occur at the normal update period specified for the MoniCA point.
 * 
 * <P>
 * Both kinds of Transactions can take an optional argument if you need to collect the data as a specific DBRType, eg
 * "DBR_STS_STRING". Doing this at the STS level also provides MoniCA with additional information such as the record's alarm
 * severity and allows UNDefined values to be recognised. If you do not specify a DBRType then operations will be performed using
 * the channel's native type, at the STS level.
 * 
 * @author David Brodrick
 */
//public class EPICS extends ExternalSystem implements ContextExceptionListener {
public class EPICS extends ExternalSystem {
  /** Logger. */
  protected static Logger theirLogger = Logger.getLogger(EPICS.class.getName());

  /** JCA context. */
  protected Context itsContext = null;

  /** Mapping between PV names and Channels. */
  protected Map<String, Channel> itsChannelMap = new ConcurrentHashMap<String, Channel>();

  /** PV names which have never been connected. */
  protected Set<String> itsNeedsConnecting = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

  /** Mapping between 'pointname:PVname' strings and EPICSListeners. */
  protected HashMap<String, EPICSListener> itsListenerMap = new HashMap<String, EPICSListener>();

  /** Records the last logged connection state of each channel. */
  protected Map<String, Boolean> itsConnectionLogMap = new ConcurrentHashMap<String, Boolean>();

  /**
   * Lists of MoniCA points which require 'monitor' updates for each PV which hasn't been connected yet.
   */
  protected HashMap<String, Vector<Object[]>> itsRequiresMonitor = new HashMap<String, Vector<Object[]>>();

  /**
   * Count of the number of channel access connections currently lost
   */
  protected AtomicInteger itsLostConnectionCount = new AtomicInteger();

  public EPICS(String[] args) {
    super("EPICS");

    // Get the JCALibrary instance.
    JCALibrary jca = JCALibrary.getInstance();
    try {
      // Create context with default configuration values.
      itsContext = jca.createContext(JCALibrary.CHANNEL_ACCESS_JAVA);
      //itsContext.addContextExceptionListener(this);
    } catch (Exception e) {
      theirLogger.error("Creating Context: " + e);
    }

    // Create thread to connect channels
    try {
      ChannelConnector connector = new ChannelConnector();
      connector.start();
    } catch (Exception e) {
      theirLogger.error("Creating ChannelConnector: " + e);
    }
  }

  /**
   * get the number of lost Channel Access Connections
   *
   * @return number of lost connections
   */
  public int getNumLostConnections() {
    return itsLostConnectionCount.get();
  }

  /**
   * get the number of pending EPICS connections
   *
   * @return number of pending conenctions
   */
  public int getNumPendingConnections() {
    return itsNeedsConnecting.size();
  }

  public int getNumActiveConnections() {
    return itsChannelMap.size();
  }

  /**
   * Poll new values from EPICS. This first ensures the EPICS channel has been established and subsequently performs an asynchronous
   * 'get' on the channel, providing the data to the MoniCA point when the 'get' callback is called.
   */
  protected void getData(PointDescription[] points) throws Exception {
    // Process each requesting point in turn
    for (int i = 0; i < points.length; i++) {
      // Get the appropriate Transaction(s) and process each PV
      Vector<Transaction> thesetrans = getMyTransactions(points[i].getInputTransactions());
      for (int j = 0; j < thesetrans.size(); j++) {
        // Get this Transaction which contains the PV name
        TransactionEPICS thistrans = (TransactionEPICS) thesetrans.get(j);
        String pvname = thistrans.getPVName();
        // Lookup the channel connected to this PV
        Channel thischan = itsChannelMap.get(pvname);
        if (thischan == null) {
          // We haven't connected to this channel yet so request its connection.
          itsNeedsConnecting.add(pvname);
          // Fire a null-data update to indicate that data is not yet available.
          points[i].firePointEvent(new PointEvent(this, new PointData(points[i].getFullName()), true));
        } else {

          String listenername = points[i].getFullName() + ":" + pvname;
          EPICSListener listener;
          synchronized (itsListenerMap) {
            // Check if we've already created the listener/callback for this point/PV combo and create if required
            listener = itsListenerMap.get(listenername);
            if (listener == null) {
              listener = new EPICSListener(thischan, points[i]);
              itsListenerMap.put(listenername, listener);
              // Ensures point gets notified if connection to PV breaks
              thischan.addConnectionListener(listener);
            }
          }

          if (thischan.getConnectionState() == Channel.ConnectionState.CONNECTED) {
            // Channel is connected, request data via a channel access 'get'

            try {
              asynchCollecting(points[i]);
              DBRType type = thistrans.getType();

              // If the Transaction doesn't specify a particular DBRType, then
              // do an initial CA get to determine the native type of the record,
              // so that we can ensure all subsequent gets use an STS DBRType
              // which ensures alarm and validity information is available.
              if (type == null) {
                DBR tempdbr = thischan.get();
                itsContext.flushIO();
                type = DBRType.forName(tempdbr.getType().getName().replaceAll("DBR_", "DBR_STS_").replaceAll("_ENUM", "_STRING"));
                thistrans.setType(type);
              }

              // Do the actual CA get operation
              thischan.get(type, 1, listener);
            } catch (Exception e) {
              // Maybe the channel just became disconnected
              points[i].firePointEvent(new PointEvent(this, new PointData(points[i].getFullName()), true));
              asynchReturn(points[i]);
            }
          } else {
            // Channel exists but is currently disconnected. Fire null-data event.
            points[i].firePointEvent(new PointEvent(this, new PointData(points[i].getFullName()), true));
          }
        }
      }
    }
    try {
      // Flush all of the get requests
      itsContext.flushIO();
    } catch (Exception e) {
      theirLogger.error("In getData method, while flushing IO: " + e);
    }
  }


  /** Send a value from MoniCA to EPICS. */
  public void putData(PointDescription desc, PointData pd) throws Exception {
    theirLogger.error("Unsupported control request from " + desc.getFullName());

    try {
      itsContext.flushIO();
    } catch (Exception e) {
      theirLogger.error("In putData method, while flushing IO: " + e);
    }
  }

  /** Register the specified point and PV name pair to receive monitor updates. */
  public void registerMonitor(PointDescription point, String pvname, DBRType type) {
    if (itsChannelMap.get(pvname) == null) {
      // Not connected to this channel yet
      itsNeedsConnecting.add(pvname);
    }
    synchronized (itsRequiresMonitor) {
      Object[] newpoint = new Object[2];
      newpoint[0] = point;
      newpoint[1] = type;
      Vector<Object[]> thesepoints = itsRequiresMonitor.get(pvname);
      if (thesepoints == null) {
        thesepoints = new Vector<Object[]>(1, 1);
        thesepoints.add(newpoint);
        itsRequiresMonitor.put(pvname, thesepoints);
      } else {
        // Check if the point is already registered to receive updates from this PV (pathological case?)
        boolean found = false;
        Iterator<Object[]> i = thesepoints.iterator();
        while (i.hasNext()) {
          if (i.next()[0] == point) {
            found = true;
            break;
          }
        }
        if (!found) {
          thesepoints.add(newpoint);
        }
      }
    }
  }

//  public void contextException(ContextExceptionEvent ev) {
//     System.out.println(ev.toString());
//  }

//  public void contextVirtualCircuitException(ContextVirtualCircuitExceptionEvent ev) {
//      System.out.println(ev.toString());
//  }

  /**
   * Thread which connects to EPICS channels and configures 'monitor' updates for points which request it.
   */
  protected class ChannelConnector extends Thread {
    /** Maximum number of channels to attempt to connect at once. */
    private final int theirMaxPending = 25000;
    /** Time between channel connection attempts, increases exponentially up to a maximum */
    private long itsCASearchPeriod = 5000000;       // usecs
    private long itsMaxCASearchPeriod = 300000000;  // usecs
    /** Channel connect timeout, decreases exponentially down to a minimum */
    private float itsCAConnectTimeout = 30;            // secs
    private float itsMinCAConnectTimeout = (float)0.2; // secs
    private AbsTime itsStartTime;

    public int itsNewChannels;
    public int itsPendingChannels;

    public ChannelConnector() {
      super("EPICS ChannelConnector");
      itsStartTime = new AbsTime();
      try {
          String str = System.getenv("EPICS_CA_MAX_SEARCH_PERIOD");
          if (str != null)
              itsMaxCASearchPeriod = Integer.parseInt(str) * 1000000;
      } catch (Exception e) {}
    }

    public void run() {
      while (itsKeepRunning) {
        // If there's no new channels then just wait
        if (itsNeedsConnecting.isEmpty() || !PointDescription.getPointsCreated()) {
          RelTime sleeptime = RelTime.factory(10000000);
          try {
            sleeptime.sleep();
          } catch (Exception e) {
          }
          continue;
        }

        // Create Channel objects for unconnected channels
        Vector<Channel> newchannels;
        ArrayList<String> asarray = new ArrayList<String>();
        synchronized (itsNeedsConnecting) {
          asarray.addAll(itsNeedsConnecting);
        }

        // Use a random start point for connecting channels
        int numnewchans = Math.min(asarray.size(), theirMaxPending);
        int startpos = (int) Math.floor((Math.random() * asarray.size()));
        newchannels = new Vector<Channel>(numnewchans);
        for (int i = 0; i < numnewchans; i++) {
          String thispv = asarray.get((i + startpos) % asarray.size());
          if (itsChannelMap.get(thispv) != null) {
            theirLogger.debug("Requested channel already exists for " + thispv);
            itsNeedsConnecting.remove(thispv);
            numnewchans--;
            continue;
          }
          try {

            // Create the Channel to connect to the PV.
            Channel thischan = itsContext.createChannel(thispv);
            newchannels.add(thischan);
           } catch (Exception e) {
            theirLogger.warn("ChannelConnector: Connecting Channel " + thispv + ": " + e);
          }
        }

        // Try to connect to the channels
        try {
          theirLogger.debug("ChannelConnector: Attempting to connect " + newchannels.size() + "/" + asarray.size() + " pending channels");
          itsNewChannels = newchannels.size();
          itsPendingChannels = asarray.size();
          itsContext.pendIO(itsCAConnectTimeout);   
        } catch (Exception e) {
          //theirLogger.debug("ChannelConnector: pendIO: " + e);
        }
	// Check if its more than 30 minutes since ChannelConnector was started
	long minutesSinceStart = (long)(((RelTime)Time.diff(new AbsTime(), itsStartTime)).getAsSeconds())/60;
	boolean enableConnectSlowdown = minutesSinceStart > 30;
	if (enableConnectSlowdown) { // Reduce the timeout after each attempt, down to a limit.
	  itsCAConnectTimeout = Math.max(itsMinCAConnectTimeout, itsCAConnectTimeout/2);
        }
	 
        // Check which channels connected successfully
        for (int i = 0; i < newchannels.size(); i++) {
          Channel thischan = newchannels.get(i);
          String thispv = thischan.getName();
          if (thischan.getConnectionState() == Channel.ConnectionState.CONNECTED) {
            // This channel connected okay
            itsChannelMap.put(thispv, thischan);
            itsNeedsConnecting.remove(thispv);
            if (itsConnectionLogMap.get(thispv) == Boolean.FALSE) {
              itsConnectionLogMap.put(thispv, Boolean.TRUE);
              itsLostConnectionCount.decrementAndGet();
            }
          } else {
            // This channel failed to connect
            try {
              if (itsConnectionLogMap.get(thispv) != Boolean.FALSE) {
                // We haven't logged a message about this PV failing to connect yet
                theirLogger.warn("ChannelConnector: Failed to connect to PV " + thispv);
                // Record that we've logged it
                itsConnectionLogMap.put(thispv, Boolean.FALSE);
                itsLostConnectionCount.incrementAndGet();
              }
              thischan.destroy();
            } catch (Exception e) {
              e.printStackTrace();
              theirLogger.error("ChannelConnector: Failed to destroy channel for " + thispv + ": " + e);
	      thischan.dispose();
            }
          }
        }

        // ////////////////////////////////////////////////////////
        // Connect any 'monitor' requests for newly established channels
        synchronized (itsRequiresMonitor) {
          Iterator<String> allreqs = itsRequiresMonitor.keySet().iterator();
          while (allreqs.hasNext()) {
            String thispv = (String) allreqs.next();
            Channel thischan = itsChannelMap.get(thispv);
            if (thischan == null) {
              // This channel is still not connected so cannot configure monitors
              continue;
            }

            // Do an initial CA get to determine the native type of the record,
            // so that we can ensure all subsequent gets use an STS DBRType
            // which ensures alarm and validity information is available.
            DBRType nativetype = null;
            try {
              DBR tempdbr = thischan.get();
              itsContext.flushIO();
              nativetype = DBRType.forName(tempdbr.getType().getName().replaceAll("DBR_", "DBR_STS_").replaceAll("_ENUM", "_STRING"));
            } catch (Exception e) {
              theirLogger.warn("ChannelConnector: ERROR determining native DBRType for " + thispv + " - bad channel state?");
              try {
                // This channel is broken. Try to reconnect later.
                itsChannelMap.put(thispv, null);        
                itsNeedsConnecting.add(thispv);
		thischan.destroy();
              } catch (Exception f) {
                theirLogger.error("ChannelConnector: Failed to destroy channel for " + thispv + ": " + e);
		thischan.dispose();
              }
              continue;
            }

            Vector<Object[]> thesepoints = itsRequiresMonitor.get(thispv);
            if (thesepoints == null || thesepoints.isEmpty()) {
              // Should never happen
              theirLogger.error("ChannelConnector: PV " + thispv + " is queued for monitor connection but no points are waiting!");
              continue;
            } else {
              Iterator<Object[]> points = thesepoints.iterator();
              while (points.hasNext()) {
                // Connect each point to 'monitor' updates from this channel
                Object[] entry = points.next();
                PointDescription pointdesc = (PointDescription) entry[0];
                DBRType thistype = (DBRType) entry[1];
                // If no DBRType explicitly specified then set native type
                if (thistype == null) {
                  thistype = nativetype;
                }

                String listenername = pointdesc.getFullName() + ":" + thispv;
                EPICSListener listener;
                try {
                  synchronized (itsListenerMap) {
                    listener = itsListenerMap.get(listenername);
                    if (listener == null) {
                      listener = new EPICSListener(thischan, pointdesc);
                      itsListenerMap.put(listenername, listener);
                      // Ensure point gets notified if connection to PV breaks
                      thischan.addConnectionListener(listener);
                    }
                  }
                  if (thistype == null) {
                    thischan.addMonitor(Monitor.VALUE | Monitor.ALARM, listener);
                  } else {
		    //theirLogger.debug("ChannelConnector: dbr type of PV " + thispv + " is " + thistype.toString());
                    // Needs to be monitored so data arrives as a specific type
                    thischan.addMonitor(thistype, 1, Monitor.VALUE | Monitor.ALARM, listener);
                  }
                  points.remove();
                } catch (Exception f) {
                  theirLogger.error("ChannelConnector: Establising Listener " + pointdesc.getFullName() + "/" + thispv + ": " + f);
                }
              }
              if (thesepoints.isEmpty()) {
                // We successfully connected all queued points for this PV
                allreqs.remove();
              }
            }
          }
        }

        // Ensure all monitor requests have been flushed
        try {
          itsContext.flushIO();
        } catch (Exception e) {
          theirLogger.error("ChannelConnector: While flushing IO: " + e);
        }
        try {
          // Sleep for a while before trying to connect remaining channels
          final RelTime sleeptime = RelTime.factory(itsCASearchPeriod);
          sleeptime.sleep();

        } catch (Exception e) {
        }
	if (enableConnectSlowdown) { // Increase time between retries for remaining failed connections
	  itsCASearchPeriod = Math.min(itsMaxCASearchPeriod, itsCASearchPeriod * 2);
	}
	//theirLogger.debug("ChannelConnector: " + minutesSinceStart + " minutes, connect timeout = " + itsCAConnectTimeout + " search period = " + itsCASearchPeriod/1000000);
      }
    }
  };

  /**
   * Class which handles asynchronous callbacks from EPICS for a specific MoniCA point.
   */
  protected class EPICSListener implements MonitorListener, GetListener, ConnectionListener {
    /** The name of the process variable we handle events for. */
    String itsPV = null;

    /** Channel which connects us to the PV. */
    Channel itsChannel = null;

    /** The point name. */
    String itsPointName = null;

    /** The MoniCA point instance. */
    PointDescription itsPoint = null;

    /** Create new instance to handle updates to the specified PV. */
    public EPICSListener(Channel chan, PointDescription point) {
      itsChannel = chan;
      itsPV = itsChannel.getName();
      itsPoint = point;
      itsPointName = point.getFullName();
    }

    /** Call back for 'monitor' updates. */
    public void monitorChanged(MonitorEvent ev) {
      if (!itsKeepRunning) {
        //Server shutdown was requested, so just dump this data
        return;
      }
      PointData pd = new PointData(itsPointName);
      try {
        if (ev.getStatus() == CAStatus.NORMAL && ev.getDBR() != null) {
          pd = getPDforDBR(ev.getDBR(), itsPointName, itsPV);
        }
      } catch (Exception e) {
        theirLogger.warn("EPICSListener.monitorChanged: " + itsPV + ": " + e);
      }
      itsPoint.firePointEvent(new PointEvent(this, pd, true));
    }

    /** Call back for 'get' updates. */
    public void getCompleted(GetEvent ev) {
      PointData pd = new PointData(itsPointName);
      try {
        if (ev.getStatus() == CAStatus.NORMAL && ev.getDBR() != null) {
          pd = getPDforDBR(ev.getDBR(), itsPointName, itsPV);
        }
      } catch (Exception e) {
        theirLogger.warn("EPICSListener.getCompleted: " + itsPV + ": " + e);
      }
      itsPoint.firePointEvent(new PointEvent(this, pd, true));
      // Return the point for rescheduling
      asynchReturn(itsPoint);
    }

    /** Call back for channel state changes. */
    public void connectionChanged(ConnectionEvent ev) {
      if (!itsConnectionLogMap.containsKey(itsPV) || itsConnectionLogMap.get(itsPV).booleanValue() != ev.isConnected()) {
        // State has changed, so log a message
        if (ev.isConnected()) {
          theirLogger.info("EPICSListener: Connection restored to PV: " + itsPV);
          itsLostConnectionCount.decrementAndGet();
        } else {
          theirLogger.warn("EPICSListener: Lost connection to PV: " + itsPV);
          itsLostConnectionCount.incrementAndGet();
        }
        // Record the state
        itsConnectionLogMap.put(itsPV, Boolean.valueOf(ev.isConnected()));
      }

      if (!ev.isConnected()) {
        // Connection just dropped out so fire null-data update
        itsPoint.firePointEvent(new PointEvent(this, new PointData(itsPointName), true));
        if (itsPoint.isCollecting()) {
          // A get was in progress so need to return the point for rescheduling
          asynchReturn(itsPoint);
        }
      }
    }
  };

  /** Decode the value from an EPICS DBR. */
  public PointData getPDforDBR(DBR dbr, String pointname, String pvname) {
    PointData pd = new PointData(pointname);
    Object newval = null;
    boolean alarm = false;
    AbsTime timestamp = new AbsTime();

    try {
      int count = dbr.getCount();
      if (count > 1) {
        theirLogger.warn("getPDforDBR: " + pvname + ": >1 value received");
      }
      Object rawval = dbr.getValue();
      // Have to switch on type, don't think there's any simpler way
      // to get the individual data as an object type.
      if (dbr instanceof INT) {
        newval = new Integer(((int[]) rawval)[0]);
      } else if (dbr instanceof BYTE) {
        newval = new Integer(((byte[]) rawval)[0]);
      } else if (dbr instanceof SHORT) {
        newval = new Integer(((short[]) rawval)[0]);
      } else if (dbr instanceof FLOAT) {
        newval = new Float(((float[]) rawval)[0]);
      } else if (dbr instanceof DOUBLE) {
        newval = new Double(((double[]) rawval)[0]);
      } else if (dbr instanceof STRING) {
        newval = ((String[]) rawval)[0];
      } else if (dbr instanceof ENUM) {
	newval = new Integer(((short[]) rawval)[0]);
	if (dbr instanceof DBR_LABELS_Enum) {
	    short idx = ((Integer)newval).shortValue();
	    String lbl = ((DBR_LABELS_Enum)dbr).getLabels()[idx];
	    newval = new EnumItem(lbl, idx);
	}
      } else {
        theirLogger.warn("getPDforDBR: " + pvname + ": Unhandled DBR type: " + dbr.getType());
      }

      // Check the alarm status, if information is available
      if (dbr instanceof STS) {
        STS thissts = (STS) dbr;
        if (thissts.getSeverity() == Severity.INVALID_ALARM) {
          // Point is undefined, so data value is invalid
          newval = null;
        } else if (thissts.getSeverity() != Severity.NO_ALARM) {
          // An alarm condition exists
          alarm = true;
        }
      }

      // Preserve the data time stamp, if available
      // / ts always seems to be null, so conversion below is untested
      /*
       * if (dbr instanceof TIME) { TIME time = (TIME)dbr; TimeStamp ts = time.getTimeStamp(); System.err.println("TS = " + ts); if
       * (ts!=null) { System.err.println("NOW=" + timestamp.toString(AbsTime.Format.UTC_STRING) + "\tTS=" + AbsTime.factory(new
       * Date(ts.secPastEpoch()*1000l + ts.nsec()/1000000l)).toString(AbsTime.Format.UTC_STRING)); timestamp = AbsTime.factory(new
       * Date(ts.secPastEpoch()*1000l + ts.nsec()/1000000l)); } }
       */

      pd.setData(newval);
      pd.setAlarm(alarm);
      pd.setTimestamp(timestamp);
    } catch (Exception e) {
      theirLogger.warn("getPDforDBR: " + pvname + ": " + e);
    }
    return pd;
  }
}
