/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.occurrence.processor.interpreting.util;

import org.gbif.common.parsers.geospatial.CellIdUtils;
import org.gbif.common.parsers.geospatial.UnableToGenerateCellIdException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper of methods for calculating cell ids for given coordinates.
 *
 * @see CellIdUtils
 */
public class CellIdCalculator {

  private static final Logger LOG = LoggerFactory.getLogger(CellIdCalculator.class);

  private CellIdCalculator() {
  }

  /**
   * Calculates the cell id for the given lat, lng.
   *
   * @param lat latitude
   * @param lng longitude
   *
   * @return cell id or null if it couldn't be calculated
   */
  public static Integer calculateCellId(Double lat, Double lng) {
    Integer cellId = null;
    if (lat != null && lng != null) {
      try {
        cellId = CellIdUtils.toCellId(lat, lng);
      } catch (UnableToGenerateCellIdException e) {
        LOG.info("Unable to generate cell id", e);
      }
    }

    return cellId;
  }

  /**
   * Calculates the centicell id for the given lat, lng.
   *
   * @param lat latitude
   * @param lng longitude
   *
   * @return centi cell id or null if it couldn't be calculated
   */
  public static Integer calculateCentiCellId(Double lat, Double lng) {
    Integer centiCellId = null;
    if (lat != null && lng != null) {
      try {
        centiCellId = CellIdUtils.toCentiCellId(lat, lng);
      } catch (UnableToGenerateCellIdException e) {
        LOG.info("Unable to generate cell id", e);
      }
    }

    return centiCellId;
  }

  /**
   * Calculates the mod360 cell id for the given longitude.
   *
   * @param lng longitude
   *
   * @return cell id or null if it couldn't be calculated
   */
  public static Integer calculateMod360CellId(Double lng) {
    Integer cellId = null;
    if (lng != null) {
      cellId = CellIdUtils.getMod360CellIdFor(lng);
    }

    return cellId;
  }
}
