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
package org.gbif.occurrence.download.service;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * Contains common constants used by the occurrence dowload classes.
 */
public final class Constants {

  // Log messages marked with this Marker should trigger an email notification
  public static final Marker NOTIFY_ADMIN = MarkerFactory.getMarker("NOTIFY_ADMIN");

  /**
   * Private constructor.
   */
  private Constants() {
    throw new UnsupportedOperationException("Can't initialize class");
  }
}
