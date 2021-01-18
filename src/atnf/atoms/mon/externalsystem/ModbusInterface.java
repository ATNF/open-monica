package atnf.atoms.mon.externalsystem;

import java.net.*;
import java.util.*;
import java.text.*;

import net.wimpi.modbus.ModbusIOException;
import net.wimpi.modbus.msg.*;
import net.wimpi.modbus.io.*;
import net.wimpi.modbus.net.*;
import net.wimpi.modbus.util.*;
import net.wimpi.modbus.procimg.SimpleRegister;
import net.wimpi.modbus.procimg.Register;

import atnf.atoms.mon.*;
import atnf.atoms.mon.transaction.*;

import org.apache.log4j.*;

/**
 * 
 * Reads and writes data to and from a Modbus/TCP interface.
 * 
 * <P>
 * The constructor requires <i>hostname:port ModbusID ModbusFunction StartAddress</i> arguments. A timeout (ms) argument may
 * optionally be specified.
 * 
 * <P>
 * NB: If control points are being used, at least one polling point should be defined as this will ensure the socket is reconnected
 * if required.
 * 
 * <P>
 * If using main standalone, the required parameters are: <i>hostname port ModbusID ModbusFunction StartAddress</i>
 * 
 * <P>
 * In both cases, the default is to read one point, but another argument is optional. This argument specifies the number of points
 * to read.
 * 
 * @author Ben McKay & Peter Mirtschin
 **/
public class ModbusInterface extends ExternalSystem {
  /** Logger. */
  private static Logger theirLogger = Logger.getLogger(ModbusInterface.class.getName());

  /** Address of the remote slave. */
  private String itsHost = null;

  /** Port to connect to the remote slave. */
  private int itsPort = net.wimpi.modbus.Modbus.DEFAULT_PORT;

  /** Handle to the actual connection. */
  private TCPMasterConnection itsConnection = null;

  /** The timeout to use (ms) */
  private int itsTimeout = 2000;

  private boolean itsDebug = false;

  public ModbusInterface(String[] args) {
    super(args[0] + ":" + args[1]);
    itsHost = args[0];
    itsPort = Integer.parseInt(args[1]);
    if (args.length > 2) {
      itsTimeout = Integer.parseInt(args[2]);
    }
  }

  /**
   * Connect to the slave.
   */
  public synchronized boolean connect() throws Exception {
    try {
      InetAddress addr = InetAddress.getByName(itsHost);
      itsConnection = new TCPMasterConnection(addr);
      itsConnection.setPort(itsPort);
      itsConnection.setTimeout(itsTimeout);
      itsConnection.connect();
      itsConnected = true;
      theirLogger.info("(" + itsHost + ":" + itsPort + "): Connected");
    } catch (Exception e) {
      //e.printStackTrace();
      theirLogger.warn("(" + itsHost + ":" + itsPort + "):" + e);
      itsConnected = false;
      throw e;
    }

    itsNumTransactions = 0;

    return itsConnected;
  }

  /**
   * Disconnect from the remote data source. This method should be overridden to achieve the required functionality.
   */
  public synchronized void disconnect() throws Exception {
    try {
      itsConnection.close();
      itsConnected = false;
    } catch (Exception e) {
      // e.printStackTrace();
      throw e;
    }
    theirLogger.info("(" + itsHost + ":" + itsPort + "): Disconnected");
  }

  public synchronized boolean setDebug(boolean on_off) throws Exception {
    itsDebug = on_off;
    return itsDebug;
  }

  private ModbusTCPTransaction createTransaction(ModbusRequest req) {
    ModbusTCPTransaction trans = new ModbusTCPTransaction(itsConnection);
    trans.setRequest(req);
    trans.setRetries(1);
    trans.setReconnecting(false);
    return trans;
  }

  /**
   * Read coils. (Modbus function 0x01) INPUT: uid = unit ID ref = starting reference count = number of coils from starting
   * reference to read
   */
  public synchronized ReadCoilsResponse readCoils(int uid, int ref, int count) throws Exception {
    // Prepare the request
    ReadCoilsRequest req = new ReadCoilsRequest(ref, count);
    req.setUnitID(uid);

    // Print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // Prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // Execute transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": readCoils: " + e);
      return null;
    }

    return (ReadCoilsResponse) trans.getResponse();
  }

  /**
   * Read discrete inputs. (Modbus function 0x02) INPUT: uid = unit ID ref = starting reference count = number of registers from
   * starting reference to read
   */
  public synchronized ReadInputDiscretesResponse readDiscreteInputs(int uid, int ref, int count) throws Exception {
    // Prepare the request
    ReadInputDiscretesRequest req = new ReadInputDiscretesRequest(ref, count);
    req.setUnitID(uid);

    // Print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // Prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // Execute transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": readDiscreteInputs: " + e);
      return null;
    }

    return (ReadInputDiscretesResponse) trans.getResponse();
  }

  /**
   * Read holding registers. (Modbus function 0x03) INPUT: uid = unit ID (for ATDS this is 1-32. (32 is DSAddr 0) ie Dataset
   * address) ref = starting reference (for ATDS this is the function code 0-511) count = number of registers from starting
   * reference to read
   */
  public synchronized ReadMultipleRegistersResponse readHoldingRegisters(int uid, int ref, int count) throws Exception {
    // Prepare the request
    ReadMultipleRegistersRequest req = new ReadMultipleRegistersRequest(ref, count);
    req.setUnitID(uid);

    // Print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // Prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // Execute transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": readHoldingRegisters: " + e);
      return null;
    }
    return (ReadMultipleRegistersResponse) trans.getResponse();
  }

  /**
   * Read input registers. (Modbus function 0x04) INPUT: uid = unit ID (for ATDS this is 1-32. (32 is DSAddr 0) ie Dataset address)
   * ref = starting reference (for ATDS this is the function code 0-511) count = number of registers from starting reference to read
   */
  public synchronized ReadInputRegistersResponse readInputRegisters(int uid, int ref, int count) throws Exception {
    // Prepare the request
    ReadInputRegistersRequest req = new ReadInputRegistersRequest(ref, count);
    req.setUnitID(uid);

    // Print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // Prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // Execute transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": readInputRegisters: " + e);
      return null;
    }
    return (ReadInputRegistersResponse) trans.getResponse();
  }

  /**
   * Write single coil. (Modbus function 0x05) INPUT: uid = unit ID ref = starting reference on_off = boolean to set coil
   */

  public synchronized WriteCoilResponse writeSingleCoil(int uid, int ref, boolean on_off) throws Exception {
    // prepare the request
    WriteCoilRequest req = new WriteCoilRequest(ref, on_off);
    req.setUnitID(uid);

    // print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // execute the transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": writeSingleCoil: " + e);
      return null;
    }
    return (WriteCoilResponse) trans.getResponse();
  }

  /**
   * Write Single register (Modbus function 0x06) INPUT: uid = unit ID (for ATDS this is 1-32. ie Dataset address) ref = starting
   * reference (for ATDS this is the function code 0-511) value = value to write to register
   */
  public synchronized WriteSingleRegisterResponse writeSingleRegister(int uid, int ref, int value) throws Exception {
    // prepare the request
    WriteSingleRegisterRequest req = new WriteSingleRegisterRequest(ref, new SimpleRegister(value));
    req.setUnitID(uid);

    // print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // execute the transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": writeSingleRegister: " + e);
      return null;
    }

    return (WriteSingleRegisterResponse) trans.getResponse();
  }

  /**
   * Write multiple coils. (Modbus function 0x0F) INPUT: uid = unit ID (for ATDS this is 1-32. ie Dataset address) ref = starting
   * reference (for ATDS this is the function code 0-511) bv = BitVector
   */
  public synchronized WriteMultipleCoilsResponse writeMultipleCoils(int uid, int ref, BitVector bv) throws Exception {
    // prepare the request
    WriteMultipleCoilsRequest req = new WriteMultipleCoilsRequest(ref, bv);
    req.setUnitID(uid);

    // print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // execute the transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": writeMultipleCoils: " + e);
      return null;
    }
    return (WriteMultipleCoilsResponse) trans.getResponse();
  }

  /**
   * Write multiple registers. (Modbus function 0x10) INPUT: uid = unit ID ref = starting reference registers
   */
  public synchronized WriteMultipleRegistersResponse writeMultipleRegisters(int uid, int ref, Register[] registers) throws Exception {
    // prepare the request
    WriteMultipleRegistersRequest req = new WriteMultipleRegistersRequest(ref, registers);
    req.setUnitID(uid);

    // print the request for debugging
    if (itsDebug)
      System.out.println("\tRequest: " + req.getHexMessage());

    // prepare the transaction
    ModbusTCPTransaction trans = createTransaction(req);

    // execute the transaction
    try {
      trans.execute();
    } catch (ModbusIOException e) {
      if (e.isEOF() || !itsConnection.isConnected()) {
        throw e;
      }
      return null;
    } catch (Exception e) {
      theirLogger.warn(itsHost + ":" + itsPort + ": writeMultipleRegisters: " + e);
      return null;
    }
    return (WriteMultipleRegistersResponse) trans.getResponse();
  }

  public void getData(PointDescription[] points) throws Exception {
    // Precondition
    if (points == null || points.length == 0)
      return;

    // theirLogger.info("(" + itsHost + ":" + itsPort + "): Monitoring = " + points.length);

    int numberToRead = 1;
    boolean useArray = false;

    for (int i = 0; i < points.length; i++) {
      PointDescription pm = points[i];
      TransactionStrings tds = (TransactionStrings) getMyTransactions(pm.getInputTransactions()).get(0);

      // Check we have correct number of arguments
      if (tds.getNumStrings() != 3 && tds.getNumStrings() != 4) {
        theirLogger.error("Modbus.getData requires either 3 or 4 arguments");
        throw new IllegalArgumentException("Modbus.getData: requires 3 or 4 arguments");
      }
      if (tds.getNumStrings() == 4) {
        numberToRead = Integer.parseInt(tds.getString(3));
        useArray = true;
      }
      else {
	useArray = false;
      }
      int UnitID = Integer.parseInt(tds.getString(0));
      int FCode = Integer.parseInt(tds.getString(1));
      int StartAddress = Integer.parseInt(tds.getString(2));

      try {
        switch (FCode) {
        case 1:
          // Read Coils
          ReadCoilsResponse rc_response = readCoils(UnitID, StartAddress, numberToRead);
          if (rc_response != null) {
            if (!useArray) {
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), new Boolean(rc_response.getCoilStatus(0))), true));
            } else {
              Boolean[] rc_Array = new Boolean[numberToRead];
              for (int j = 0; j < (numberToRead); j++)
                rc_Array[j] = new Boolean(rc_response.getCoilStatus(j));
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), rc_Array), true));
            }
          } else {
            // No response
            pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName()), true));
          }
          break;
        case 2:
          // Read Discrete Inputs
          ReadInputDiscretesResponse di_response = readDiscreteInputs(UnitID, StartAddress, numberToRead);
          if (di_response != null) {
            if (!useArray) {
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), new Boolean(di_response.getDiscreteStatus(0))), true));
            } else {
              Boolean[] di_Array = new Boolean[numberToRead];
              for (int j = 0; j < (numberToRead); j++)
                di_Array[j] = new Boolean(di_response.getDiscreteStatus(j));
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), di_Array), true));
            }
          } else {
            // No response
            pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName()), true));
          }
          break;
        case 3:
          // Read Holding Registers (jamod library terminology uses multiple instead of holding)
          ReadMultipleRegistersResponse rhr_response = readHoldingRegisters(UnitID, StartAddress, numberToRead);
          if (rhr_response != null) {
            if (!useArray) {
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), new Integer(rhr_response.getRegisterValue(0))), true));
            } else {
              Integer[] rhr_Array = new Integer[numberToRead];
              for (int j = 0; j < (numberToRead); j++)
                rhr_Array[j] = new Integer(rhr_response.getRegisterValue(j));
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), rhr_Array), true));
            }
          } else {
            // No response
            pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName()), true));
          }
          break;
        case 4:
          // Read Input Registers
          ReadInputRegistersResponse rir_response = readInputRegisters(UnitID, StartAddress, numberToRead);
          if (rir_response != null) {
            if (!useArray) {
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), new Integer(rir_response.getRegisterValue(0))), true));
            } else {
              Integer[] rir_Array = new Integer[numberToRead];
              for (int j = 0; j < (numberToRead); j++)
                rir_Array[j] = new Integer(rir_response.getRegisterValue(j));
              pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), rir_Array), true));
            }
          } else {
            // No response
            pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName()), true));
          }
          break;
        default:
          theirLogger.warn("Modbus.getData: Unknown Modbus monitor function code: " + FCode);
          pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName(), null), true));
          break;
        }
      } catch (Exception f) {
        theirLogger.error("(" + itsHost + ":" + itsPort + "): getData for point " + pm.getFullName() + ": " + f);
        disconnect();
        return;
      }

    }
    // Increment the transaction counter for this ExternalSystem
    itsNumTransactions++;

  }

  /**
   * Write a value to the device.
   * 
   * @param desc
   *          The point that requires the write operation.
   * @param pd
   *          The value that needs to be written.
   * @throws Exception
   */
  public void putData(PointDescription desc, PointData pd) throws Exception {
    // Perform some initial checks
    if (pd.getData() == null) {
      throw new IllegalArgumentException("Dataset.putData: Data cannot be null for " + desc.getFullName());
    }

    PointDescription pm = desc;
    TransactionStrings tds = (TransactionStrings) getMyTransactions(pm.getOutputTransactions()).get(0);
    int UnitID = Integer.parseInt(tds.getString(0));
    int FCode = Integer.parseInt(tds.getString(1));
    int StartAddress = Integer.parseInt(tds.getString(2));
    ModbusResponse resp = null;

    try {
      switch (FCode) {
      case 5: // Write single coil
        // Make sure data is boolean
        if (!(pd.getData() instanceof Boolean)) {
          theirLogger.error("Modbus.putData: Data must be Boolean for FCode 5");
          throw new IllegalArgumentException("Modbus.putData: Data must be Boolean for FCode 5");
        }
        boolean BoolOut = ((Boolean) pd.getData()).booleanValue();
        resp = writeSingleCoil(UnitID, StartAddress, BoolOut);
        // theirLogger.info("Write coil response: " + wc_response.getHexMessage());
        break;
      case 6: // Write single register
        // Make sure data is number
        if (!(pd.getData() instanceof Number)) {
          theirLogger.error("Modbus.putData: Data must be Number for FCode 6");
          throw new IllegalArgumentException("Modbus.putData: Data must be Number for FCode 6");
        }
        int NumOut = ((Number) pd.getData()).intValue();
        resp = writeSingleRegister(UnitID, StartAddress, NumOut);
        // theirLogger.info("Write single reg response: " + wsr_response1.getHexMessage() );
        break;
      case 15:
        // Write multiple coils
        // For now, only allow single coil
        theirLogger.warn("Modbus: write multiple coils is currently not supported");
        break;
      case 16:
        // Write multiple registers
        // For now, only allow single register
        theirLogger.warn("Modbus: write multiple registers is currently not supported");
        break;
      default:
        theirLogger.warn("Unknown Modbus write function code: " + FCode);
        break;
      }

      if (resp == null) {
        theirLogger.warn("(" + itsHost + ":" + itsPort + "): putData for point " + pm.getFullName() + ": No Response");
      }

    } catch (Exception f) {
      theirLogger.error("(" + itsHost + ":" + itsPort + "): putData: " + f);
      disconnect();
      return;
    }

  }

  // Print usage.
  private static void usage() {
    System.err.println("usage:");
    System.err.println("  ??? hostname port id type start [count]");
    System.err.println("where:");
    System.err.println("  Hostname: Modbus host IP address");
    System.err.println("  Port: Modbus port (usually 502)");
    System.err.println("  ID: Modbus ID (1-255)");
    System.err.println("  Type: 1:ReadCoils, 2:ReadDiscreteInputs, 3:ReadHoldingRegisters, 4:ReadInputRegisters");
    System.err.println("  Start: Modbus Start Address (0-65535)");
    System.err.println("  Count: Number of points to read (optional). Default:1 (1-125)");
    System.exit(1);
  }

  public static void main(String[] args) {
    // disable logging messages
    theirLogger.setLevel(Level.OFF);

    try {

      // Check for correct number and value of each argument.
      int numberToRead = 0;
      if (args.length == 5) {
        numberToRead = 1;
      } else if (args.length == 6) {
        numberToRead = Integer.parseInt(args[5]);
        if (numberToRead < 1 || numberToRead > 125)
          usage();
      } else {
        usage();
      }

      int UnitID = Integer.parseInt(args[2]);
      if (UnitID < 1 || UnitID > 255)
        usage();
      int FCode = Integer.parseInt(args[3]);
      if (FCode < 1 || FCode > 4)
        usage();
      int StartAddress = Integer.parseInt(args[4]);
      if (StartAddress < 0 || StartAddress > 65535)
        usage();

      ModbusInterface mbi = new ModbusInterface(args);
      mbi.connect();

      // Print Time
      Date dNow = new Date();
      SimpleDateFormat ft = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS"); // zzz");
      System.out.print(ft.format(dNow));

      try {
        switch (FCode) {
        case 1:
          // Read Coils
          ReadCoilsResponse rc_response = mbi.readCoils(UnitID, StartAddress, numberToRead);
          for (int i = 0; i < numberToRead; i++) {
            System.out.print("," + new Boolean(rc_response.getCoilStatus(i)).toString());
          }
          break;
        case 2:
          // Read Discrete Inputs
          ReadInputDiscretesResponse di_response = mbi.readDiscreteInputs(UnitID, StartAddress, numberToRead);
          for (int i = 0; i < numberToRead; i++) {
            System.out.print("," + new Boolean(di_response.getDiscreteStatus(i)).toString());
          }
          break;
        case 3:
          // Read Holding Registers (jamod library terminology uses multiple instead of holding)
          ReadMultipleRegistersResponse rhr_response = mbi.readHoldingRegisters(UnitID, StartAddress, numberToRead);
          for (int i = 0; i < numberToRead; i++) {
            System.out.print("," + new Integer(rhr_response.getRegisterValue(i)).toString());
          }
          break;
        case 4:
          // Read Input Registers
          ReadInputRegistersResponse rir_response = mbi.readInputRegisters(UnitID, StartAddress, numberToRead);
          for (int i = 0; i < numberToRead; i++) {
            System.out.print("," + new Integer(rir_response.getRegisterValue(i)).toString());
          }
          break;
        default:
          theirLogger.warn("Modbus.getData: Unknown Modbus monitor function code: " + FCode);
          break;
        }
      } catch (Exception f) {
        return;
      }

      System.out.println();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
