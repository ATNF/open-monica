// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.translation;

import atnf.atoms.mon.*;

/**
 * Extract a string from ASCII characters packed into an array of long integers.
 * 
 * Requires three arguments:
 * <ol>
 * <li><b>Order:</b> Determines the order to read in characters from each array element ("0" = right to left, "1" = left to right).
 * <li><b>Start Index:</b> The start index of the string in the input array of modbus registers.
 * <li><b>Length:</b> The number of consecutive modbus registers which contain the string data.
 * </ol>
 * 
 * @author David Brodrick
 **/
public class TranslationBytesToString extends Translation {

  private int itsOrder;
  private int itsStartIndex;
  private int itsLength;

  public TranslationBytesToString(PointDescription parent, String[] init) {
    super(parent, init);

    if (init.length != 3) {
      throw new IllegalArgumentException("TranslationBytesToString: Requires three arguments");
    } else {
      itsOrder = Integer.parseInt(init[0]);
      itsStartIndex = Integer.parseInt(init[1]);
      itsLength = Integer.parseInt(init[2]);
    }
  }

  public PointData translate(PointData data) {

    // Precondition
    if (data == null)
      return null;

    // Get the full array
    Object[] array = (Object[]) data.getData();

    // Create the new data structure to be returned
    PointData res = new PointData(itsParent.getFullName());
    // Keep the time-stamp of the parent point rather than use "now"
    res.setTimestamp(data.getTimestamp());

    // If the data is null we need to throw a null-data result
    if (array == null)
      return res;

    String strval = "";
    for (int i = 0; i < itsLength && itsStartIndex + i < array.length; i++) {
      Long thisreg = ((Number) array[itsStartIndex + i]).longValue();
      
      switch (itsOrder) {
        case 0:  // Contents of each array element are read right to left
          for (int j = 0; j < 64; j += 8) {
            // Convert this byte into a string character
            byte thisbyte = (byte) ((thisreg >> j) & 0xFF);
            if (thisbyte != 0) {
              strval += (char) thisbyte;
            }
          }
        case 1:  // Contents of each array element are read left to right
          for (int j = 56; j >= 0; j -= 8) {
            // Convert this byte into a string character
            byte thisbyte = (byte) ((thisreg >> j) & 0xFF);
            if (thisbyte != 0) {
              strval += (char) thisbyte;
            }
          }
        default:  // invalid order value
      }
      
    }
    // Convert to double and set point to return
    res.setData(strval);

    return res;
  }
}
