//
// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.externalsystem;

import atnf.atoms.time.AbsTime;
import atnf.atoms.time.DUTC;
import atnf.atoms.time.Time;
import atnf.atoms.mon.*;
import atnf.atoms.mon.transaction.*;
import atnf.atoms.mon.archiver.PointArchiver;
import atnf.atoms.mon.archiver.PointArchiverInfluxDb;

/**
 * Used to return data about the MoniCA server.
 * 
 * Points using this should have a TransactionStrings with the channel set to "system" and a second argument which determines the
 * data to be retrieved. This may be one of the following:
 * 
 * <ul>
 * <li><b>time</b> Return the current time on the server.
 * <li><b>dUTC</b> The current dUTC as known internally by the server.
 * <li><b>points</b> Return the current number of points defined on the server.
 * <li><b>systems</b> Return the current number of external systems defined on the system.
 * <li><b>uptime</b> The elapsed time since the server was started.
 * <li><b>EPICS_ActiveConnections</b> The number of connected EPICS Channel Access connections.
 * <li><b>EPICS_PendingConnections</b> The number of outstanding EPICS Channel Access connections.
 * <li><b>EPICS_LostConnections</b> The current number of lost EPICS Channel Access connections.
 * <li><b>InfluxDB_IngestRate</b> The ingest rate to InfluxDB in points per second
 * <li><b>InfluxDB_IngestLatency</b> The time difference between a point data timestamp and ingest time
 * <li><b>InfluxDB_UnmappedPoints</b>Number of MoniCA points that didn't match any Influx mapping
 * </ul>
 * 
 * @author David Brodrick
 **/
class MoniCAInternal extends ExternalSystem {
  /** The time the server started. */
  private AbsTime itsStartTime;

  public MoniCAInternal(String[] args) {
    super("system");

    // Record the system start time
    itsStartTime = new AbsTime();
  }

  protected void getData(PointDescription[] points) throws Exception {
    try {
      for (int i = 0; i < points.length; i++) {
        PointDescription desc = points[i];
        // Get the Transactions which associates the point with us
        TransactionStrings thistrans = (TransactionStrings) getMyTransactions(desc.getInputTransactions()).get(0);
        PointData pd = new PointData(desc.getFullName(), AbsTime.factory(), null);

        if (thistrans.getString().equals("time")) {
          pd.setData(pd.getTimestamp());
        } else if (thistrans.getString().equals("points")) {
          pd.setData(new Integer(PointDescription.getAllPoints().size()));
        } else if (thistrans.getString().equals("systems")) {
          pd.setData(new Integer(ExternalSystem.getAllExternalSystems().size()));
        } else if (thistrans.getString().equals("uptime")) {
          pd.setData(Time.diff(new AbsTime(), itsStartTime));
        } else if (thistrans.getString().equals("dUTC")) {
          pd.setData(DUTC.get());
        } else if (thistrans.getString().startsWith("EPICS")) {
          EPICS epics = (EPICS) ExternalSystem.getExternalSystem("EPICS");
          if (epics == null) {
            // don't have EPICS so ignore these points
            continue;
          }
          if (thistrans.getString().equals("EPICS_ActiveConnections")) {
            pd.setData(epics.getNumActiveConnections());
          } else if (thistrans.getString().equals("EPICS_PendingConnections")) {
            pd.setData(epics.getNumPendingConnections());
          } else if (thistrans.getString().equals("EPICS_LostConnections")) {
            pd.setData(epics.getNumLostConnections());
          }
        }
        else if (thistrans.getString().startsWith("InfluxDB")) {
            if (!(PointArchiver.getPointArchiver() instanceof PointArchiverInfluxDb)) {
              // InfluxDB archiver not active so ignore
              continue;
            }
            PointArchiverInfluxDb influx = (PointArchiverInfluxDb)PointArchiver.getPointArchiver();
            if (thistrans.getString().equals("InfluxDB_UnmappedPoints")) {
              pd.setData(influx.getUnmappedPoints());
            } else if (thistrans.getString().equals("InfluxDB_IngestRate")) {
              pd.setData(influx.getIngestRate());
            } else if (thistrans.getString().equals("InfluxDB_IngestLatency")) {
              pd.setData(influx.getIngestLatency());
          }
        }

        desc.firePointEvent(new PointEvent(this, pd, true));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
