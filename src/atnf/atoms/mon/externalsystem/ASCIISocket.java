// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.externalsystem;

import java.io.*;
import java.net.SocketTimeoutException;
import atnf.atoms.mon.*;
import atnf.atoms.mon.transaction.TransactionStrings;
import atnf.atoms.time.RelTime;

import org.apache.log4j.Logger;

/**
 * Class for ExternalSystems which need to read and/or write ASCII data over TCP socket connection to a remote end-point. This class
 * derives from the TCPSocket class but also provides BufferedReader and BufferedWriter fields.
 * <P>
 * The constructor argument defined in <tt>monitor-sources.txt</tt> must include the remote machine, port and optionally timeout in
 * <tt>host:port:timeout_ms:your_other_args</tt> format.
 * <P>
 * You can override the <tt>parseData</tt> method to perform whatever response parsing and/or request formatting is required, and
 * return an appropriate data object. Alternately you may override the <i>getData</i> method in which case the <i>parseData</i>
 * method will not be called.
 * <P>
 * The default <tt>parseData</tt> implementation expects the points to have a <tt>TransactionStrings</tt> input transaction, which
 * has the query string to be sent over the socket. An optional additional argument can specify the number of lines expected in the
 * response, this is useful when the server has inter-line latency which can trick the standard logic into thinking that the entire
 * response has been consumed. All response lines will be concatenated and returned as the data object.
 * 
 * @author David Brodrick
 */
public class ASCIISocket extends TCPSocket {
  /** The output stream for writing text to the remote service. */
  protected BufferedWriter itsWriter = null;

  /** The input stream for reading responses from the remote service. */
  protected BufferedReader itsReader = null;

  /** A counter for the number of consecutive socket read timeouts. */
  protected int timeoutCounter = 0;

  /** Argument must include host:port and optionally :timeout_ms */
  public ASCIISocket(String[] args) {
    super(args);
  }

  /** Make a new socket connection. */
  public boolean connect() throws Exception {
    try {
      super.connect();
      itsWriter = new BufferedWriter(new OutputStreamWriter(itsSocket.getOutputStream()));
      itsReader = new BufferedReader(new InputStreamReader(itsSocket.getInputStream()));
      Logger logger = Logger.getLogger(this.getClass().getName());
      logger.info("Connected to " + itsHostName + ":" + itsPort);
    } catch (Exception e) {
      itsReader = null;
      itsWriter = null;
      super.disconnect();
      throw e;
    }
    return itsConnected;
  }

  /** Close the socket, unless it is already closed. */
  public void disconnect() throws Exception {
    itsReader = null;
    itsWriter = null;
    super.disconnect();
  }

  /** */
  public synchronized void putData(PointDescription desc, PointData pd) throws Exception {
    if (!isConnected()) {
      connect();
    }

    try {
      // Get the Transaction which associates the point with us
      TransactionStrings thistrans = (TransactionStrings) getMyTransactions(desc.getOutputTransactions()).get(0);

      // The Transaction should contain a query string to be issued to the
      // server
      if (thistrans.getNumStrings() < 1) {
        throw new Exception("ASCIISocket: Not enough arguments in Transaction");
      }

      String cmdstring = thistrans.getString();
      // Substitute EOL characters
      cmdstring = cmdstring.replaceAll("\\\\n", "\n").replaceAll("\\\\r", "\r");
      // Substitute the data value for $V
      cmdstring = cmdstring.replaceAll("\\$V", pd.getData().toString());

      // Send the command to the server
      itsWriter.write(cmdstring);
      itsWriter.flush();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * You can override this method in your class to perform any response parsing and/or request formatting that is required. This
   * default implementation sends the request string to the server and then returns all response lines as a single string.
   */
  public Object parseData(PointDescription requestor) throws Exception {
    String result = null;

    // Get the Transaction which associates the point with us
    TransactionStrings thistrans = (TransactionStrings) getMyTransactions(requestor.getInputTransactions()).get(0);

    // The Transaction should contain a query string to be issued to the server
    if (thistrans.getNumStrings() < 1) {
      throw new Exception("ASCIISocket: Not enough arguments in Transaction");
    }
    String query = thistrans.getString();
    // Substitute EOL characters
    query = query.replaceAll("\\\\n", "\n").replaceAll("\\\\r", "\r");

    try {
      // Clear input buffer
      while (itsReader.ready()) {
        itsReader.readLine();
      }

      // Send the query to the server
      itsWriter.write(query);
      itsWriter.flush();

      // Check if the Transaction specifies the number of reply lines to expect
      int numexpected = -1;
      if (thistrans.getNumStrings() > 1) {
        numexpected = Integer.parseInt(thistrans.getString(1));
      }

      // Read response
      result = itsReader.readLine() + "\n";
      int numread = 1;
      while (itsReader.ready() || (numexpected != -1 && numread < numexpected)) {
        String line = itsReader.readLine() + "\n";
        result = result + line;
        numread++;
      }
      timeoutCounter = 0;
    } catch (SocketTimeoutException ste) { // End device probably offline / powered-down ... no need to disconnect socket immediately
      if (++timeoutCounter > 10) {
        timeoutCounter = 0;
	throw ste;
      }
    }

    return result;
  }

  /** Collect data and fire events to queued monitor points. */
  protected synchronized void getData(PointDescription[] points) throws Exception {
    try {
      for (int i = 0; i < points.length; i++) {
        Object o;
        PointDescription pm = points[i];
        o = parseData(pm);
        // Count successful transactions
        if (o != null) {
          itsNumTransactions++;
        }
        // Fire the new data off for this point
        pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), o), true));
      }
    } catch (Exception e) {
      // Probably a comms error.
      disconnect();
      Logger logger = Logger.getLogger(this.getClass().getName());
      logger.error("(" + itsName + "): " + e);
    }
  }
}
