package atnf.atoms.mon.externalsystem;

import org.apache.log4j.*;
import org.snmp4j.*;
import org.snmp4j.smi.*;
import org.snmp4j.event.*;
import org.snmp4j.security.*;
import org.snmp4j.mp.*;
import org.snmp4j.transport.*;
import org.snmp4j.util.DefaultPDUFactory;

import atnf.atoms.mon.*;
import atnf.atoms.mon.transaction.TransactionStrings;

/**
 * Generic SNMP interface supporting SNMPv1, v2c and NOAUTH, NOPRIV SNMPv3
 * requests, using a provided username. It should be straightforward to extend
 * the class to support write operations and enable fully encrypted SNMPv3
 * support. This class uses the SNMP4J library.
 * 
 * <P>
 * The constructor expects the following arguments:
 * <ul>
 * <li><b>Host Name:</b> The name or IP address of the agent.
 * <li><b>UDP Port:</b> The UDP port (usually 161).
 * <li><b>SNMP Version:</b> "v1", "v2c" or "v3".
 * <li><b>Ident:</b> The username or community, depending on which SNMP version
 * you are using.
 * </ul>
 * 
 * <P>
 * Here is an example entry for <tt>monitor-sources.txt</tt> which connects to
 * "labswitch" using username "dlink".
 * <P>
 * <tt>SNMP labswitch:161:v3:dlink</tt>
 * 
 * <P>
 * The ExternalSystem instances register their channel id's as "snmp-host:port",
 * where host and port are the values provided.
 * 
 * <P>
 * Any points which use SNMP to collect their data need to use a
 * <tt>TransactionStrings</tt> and set the first argument after the channel id
 * to be the OID of the data point to be collected in dot notation. For instance
 * like this <tt>Strings-"snmp-$1:161""1.3.6.1.2.1.1.3.0"</tt>.
 *
 * <P>
 * For set/assignment operations, the relevant TransactionStrings must be in the
 * output transactions field, and must contain an additional string being the 
 * SNMP data type to write the value as. For instance:
 * <tt>Strings-"snmp-192.168.1.113:161""1.3.6.1.4.1.32111.1.1.2.5.0""Integer32"</tt>
 * 
 * @author David Brodrick
 */
public class SNMP extends ExternalSystem {
  /** Logger. */
  private static Logger theirLogger = Logger.getLogger(SNMP.class.getName());

  /** The remote host name. */
  protected String itsHostName;

  /** The remote port. */
  protected int itsPort;

  /** The SNMPv3 user name or v1/2 community. */
  protected String itsIdent;

  /** The SNMP Target. */
  protected Target itsTarget;

  /** The SNMP instance. */
  protected Snmp itsSNMP;

  /** The different SNMP versions supported. */
  public static enum SNMPVersion {
    v1, v2c, v3
  };

  /** The SNMP version to use. */
  protected SNMPVersion itsVersion;

  public SNMP(String[] args) {
    super("snmp-" + args[0] + ":" + args[1]);
    itsHostName = args[0];
    itsPort = Integer.parseInt(args[1]);
    itsVersion = SNMPVersion.valueOf(SNMPVersion.class, args[2]);
    itsIdent = args[3];

    try {
      TransportMapping transport = new DefaultUdpTransportMapping();
      MessageDispatcher disp = new MessageDispatcherImpl();
      disp.addMessageProcessingModel(new MPv1());
      disp.addMessageProcessingModel(new MPv2c());
      disp.addMessageProcessingModel(new MPv3());
      itsSNMP = new Snmp(disp, transport);

      if (itsVersion == SNMPVersion.v3) {
        USM usm = new USM(SecurityProtocols.getInstance(), new OctetString(MPv3.createLocalEngineID()), 0);
        SecurityModels.getInstance().addSecurityModel(usm);
        itsSNMP.getUSM().addUser(new OctetString(itsIdent), new UsmUser(new OctetString(itsIdent), null, null, null, null));
      }
      transport.listen();

      Address targetAddress = GenericAddress.parse("udp:" + itsHostName + "/" + itsPort);

      if (itsVersion == SNMPVersion.v3) {
        itsTarget = new UserTarget();
        itsTarget.setVersion(SnmpConstants.version3);
        ((UserTarget) itsTarget).setSecurityLevel(SecurityLevel.NOAUTH_NOPRIV);
        ((UserTarget) itsTarget).setSecurityName(new OctetString(itsIdent));
      } else {
        itsTarget = new CommunityTarget();
        if (itsVersion == SNMPVersion.v1) {
          itsTarget.setVersion(SnmpConstants.version1);
        } else {
          itsTarget.setVersion(SnmpConstants.version2c);
        }
        ((CommunityTarget) itsTarget).setCommunity(new OctetString(itsIdent));
      }

      itsTarget.setAddress(targetAddress);
      itsTarget.setRetries(1);
      itsTarget.setTimeout(5000);

      itsConnected = true;
    } catch (Exception e) {
      theirLogger.fatal("Error while creating SNMP classes: " + e);
      itsConnected = false;
    }
  }
  
  public void putData(PointDescription pm, PointData pd) throws Exception {
    TransactionStrings tds = (TransactionStrings) getMyTransactions(pm.getOutputTransactions()).get(0);

    // Check we have correct number of arguments
    if (tds.getNumStrings() < 2) {
      theirLogger.error("(" + itsHostName + "): Expect OID and Type Code argument in Transaction for point \"" + pm.getFullName() + "\"");
      throw new IllegalArgumentException("Missing OID and Type Code argument in Transaction");
    }
    
    // Get the value to assign
    AbstractVariable newval = getSNMPVariable(tds.getString(1), pd);
    if (newval!=null) {
      // Create an OID from the string argument
      OID oid = new OID(tds.getString());

      // Send the SNMP request
      PDU pdu = DefaultPDUFactory.createPDU(itsTarget, PDU.SET);
      pdu.add(new VariableBinding(oid, newval));
      ResponseEvent response = itsSNMP.send(pdu, itsTarget);

      // Process response
      PDU responsePDU = response.getResponse();
      if (responsePDU == null || responsePDU.getErrorStatus() != SnmpConstants.SNMP_ERROR_SUCCESS || !responsePDU.get(0).getOid().equals(oid)) {
        // Response timed out or was in error
        theirLogger.warn("While setting " + itsHostName + ":" + tds.getString() + ":" + responsePDU);
      }    

      // Increment the transaction counter for this ExternalSystem
      itsNumTransactions++;
    }
  }
  
  public void getData(PointDescription[] points) throws Exception {
    for (int i = 0; i < points.length; i++) {
      PointDescription pm = points[i];
      TransactionStrings tds = (TransactionStrings) getMyTransactions(pm.getInputTransactions()).get(0);
      ResponseEvent response;

      try {
        // Check we have correct number of arguments
        if (tds.getNumStrings() < 1) {
          theirLogger.error("(" + itsHostName + "): Expect OID argument in Transaction for point \"" + pm.getFullName() + "\"");
          throw new IllegalArgumentException("Missing OID argument in Transaction");
        }
        // Create an OID from the string argument
        OID oid = new OID(tds.getString());

        // Send the SNMP request
        PDU pdu = DefaultPDUFactory.createPDU(itsTarget, PDU.GET);
        pdu.add(new VariableBinding(oid));
        response = itsSNMP.send(pdu, itsTarget); 

        // Process response
        PDU responsePDU = response.getResponse();
        PointData newdata;
      
        //theirLogger.debug(itsHostName + " PDU error status: " + responsePDU.getErrorStatus());

        if (responsePDU == null || responsePDU.getErrorStatus() != SnmpConstants.SNMP_ERROR_SUCCESS || !responsePDU.get(0).getOid().equals(oid)) {
          // Response timed out or was in error, so fire event with null data
          newdata = new PointData(pm.getFullName());
        } else {
          // Fire event with new data value (always as a string)
          newdata = new PointData(pm.getFullName(), responsePDU.get(0).getVariable().toString());
        }
        pm.firePointEvent(new PointEvent(this, newdata, true));

        // Increment the transaction counter for this ExternalSystem
        itsNumTransactions++;
      } catch (Exception e) {
        // This is triggered when the SNMP host is unreachable.
        // need to fire PointEvent with null data, otherwise OutOfMemory error brings down MoniCA.`
        theirLogger.fatal("Caught error: " + e + " for point " + pm.getFullName());
        pm.firePointEvent(new PointEvent(this, new PointData(pm.getFullName()), true));
      }

    }
  }
  
  protected AbstractVariable getSNMPVariable(String typecode, PointData pd) {
    if (pd==null || pd.getData()==null) {
      return null;
    } else if (typecode.equals("OctetString")) {
      return new OctetString(pd.getData().toString());
    } else if (typecode.equals("Integer32") && pd.getData() instanceof Number) {
      return new Integer32(((Number)pd.getData()).intValue());
    } else if (typecode.equals("Counter32") && pd.getData() instanceof Number) {
      return new Counter32(((Number)pd.getData()).intValue());      
    } else {
      theirLogger.warn("Unhandled type code/data value: \"" + typecode + "\" with " + pd);
      return null;
    }
  }

  public static void usage() {
      System.out.println("\nusage: SNMP source oid1 [oid2 oid3 ...]");
      System.out.println("\twhere\tsource comprises hostname:port:snmpVer:community");
      System.out.println("\t\ti.e. the moniCA monitor-source name including the colons.");
      System.out.println("\t\tIf only hostname is supplied, the defaults are:-");
      System.out.println("\t\t\tport = 161");
      System.out.println("\t\t\tsnmpVer = v2c");
      System.out.println("\t\t\tcommunity = public");      
      System.out.println("\t\toid is the snmp object id string");
      System.out.println();
      System.exit(1);
  }

  public static void main(String[] args) {

    ResponseEvent response;

    if (args.length < 2) usage();
    // disable logging messages
    theirLogger.setLevel(Level.OFF);
    String[] source = {"host", "161", "v2c", "public"};
    String[] tmp = args[0].split(":");
    if (tmp.length < 1 || tmp.length > 4) usage();
    for (int i = 0; i < tmp.length; ++i) {
      source[i] = tmp[i];
    }
    String[] oids = new String[args.length-1];
    for (int i = 1; i < args.length; ++i) {
      oids[i-1] = args[i];
    }

    try {
      SNMP snmpTest = new SNMP(source);
      for (int i = 0; i < oids.length; ++i) {
        // Create an OID from the string argument  
        OID oid = new OID(oids[i]);
        // Send the SNMP request
        PDU pdu = DefaultPDUFactory.createPDU(snmpTest.itsTarget, PDU.GET);
        pdu.add(new VariableBinding(oid));
        response = snmpTest.itsSNMP.send(pdu, snmpTest.itsTarget); 
        // Process response
        PDU responsePDU = response.getResponse();
        PointData newdata;  
        if (responsePDU != null) {    
          if (responsePDU.getErrorStatus() != SnmpConstants.SNMP_ERROR_SUCCESS || !responsePDU.get(0).getOid().equals(oid)) {
            // Response error
            System.out.println("ERROR: " + snmpTest.itsHostName + " PDU error status: " + responsePDU.getErrorStatus());
          } else {
            System.out.println("Success !");
            newdata = new PointData(snmpTest.getName(), responsePDU.get(0).getVariable().toString());
            System.out.println("\t" + newdata.toString() + "\n");
          }
        
        }
        else {
          System.out.println("FAILED: Timed out !");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  
  }

}
