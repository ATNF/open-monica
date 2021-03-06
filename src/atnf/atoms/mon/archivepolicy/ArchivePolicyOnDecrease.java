// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.
//

package atnf.atoms.mon.archivepolicy;

import atnf.atoms.mon.*;

import org.apache.log4j.Logger;

/**
 * Archives data when the numeric value decreases.
 * 
 * @author: David Brodrick
 */
public class ArchivePolicyOnDecrease extends ArchivePolicy
{
  /** The last data value. */
  Number itsLastData = null;

  public ArchivePolicyOnDecrease(PointDescription parent, String[] args)
  {
    super(parent, args);
  }

  public boolean checkArchiveThis(PointData data)
  {
    if (data.getData() != null && !(data.getData() instanceof Number)) {
      Logger logger = Logger.getLogger(this.getClass().getName());
      logger.error("(" + itsParent.getFullName() + "): Require Numeric data for input point " + data.getName());
      return false;
    }

    Number newdata = (Number) data.getData();
    Number olddata = itsLastData;
    itsLastData = newdata;

    if (olddata == null || newdata == null) {
      return false;
    } else {
      if (newdata.doubleValue() < itsLastData.doubleValue()) {
        return true;
      } else {
        return false;
      }
    }
  }
}
