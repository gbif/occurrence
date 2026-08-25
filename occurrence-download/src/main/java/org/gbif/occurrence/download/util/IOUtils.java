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
package org.gbif.occurrence.download.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;

/**
 * Small stream-copy helpers, replacing the commons-compress/commons-io IOUtils.copy methods used
 * in this module.
 */
public final class IOUtils {

  private IOUtils() {}

  public static void copy(InputStream input, OutputStream output, int bufferSize) throws IOException {
    byte[] buffer = new byte[bufferSize];
    int n;
    while ((n = input.read(buffer)) != -1) {
      output.write(buffer, 0, n);
    }
  }

  public static void copy(InputStream input, Writer output, Charset charset) throws IOException {
    Reader reader = new InputStreamReader(input, charset);
    char[] buffer = new char[4096];
    int n;
    while ((n = reader.read(buffer)) != -1) {
      output.write(buffer, 0, n);
    }
  }
}