MoniCA is a versatile real-time monitor and control system originally developed for use at the [Compact Array](http://www.narrabri.atnf.csiro.au/) and other radio observatories operated by Australia's [CSIRO](http://www.csiro.au/).

The server allows data to be collected from an essentially unlimited range of devices and archived to disk. Control loops can also be implemented, with calculated values being written back to devices. A graphical client program allows real-time and historical data to be displayed in a flexible manner, printed, or exported for deeper analysis.

Many features are provided out of the box, but the framework also makes it easy for you to incorporate data from new devices or to handle data in special ways.

Some features:
  * Generic support for industry standard protocols such as Modbus, SNMP and EPICS.
  * Combine data in arbitrary ways to create aggregate monitor points.
  * Alarm management system, with notifications, alarm acknowledgement, etc.
  * Control points allow operation of remote devices to be implemented.
  * Optionally archive data to disk, using a compressed format or a MySQL database.
  * The GUI client can plot any combination of monitor points and display real-time values in tables.
  * Client fully tested on Linux, Windows, Mac and Solaris operating systems.
  * Javascript-based web browser client included in the distribution.
  * Server has a ZeroC Ice interface and can be accessed natively from a number of different programming languages.
  * Server has a simple ASCII socket as an alternate way to provide data to external programs.
  * Used at research facilities with tens of thousands of real-time monitor points.
  * Easily export data for analysis in spreadsheets or take graphical screen-shots.

The Downloads section is mainly for historical reference, if you would like to give MoniCA a try then please check out the [Installation](../../wiki/Installation) wiki page.
