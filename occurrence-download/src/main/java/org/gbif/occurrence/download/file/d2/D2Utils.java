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
package org.gbif.occurrence.download.file.d2;

import org.gbif.hadoop.compress.d2.D2CombineInputStream;
import org.gbif.hadoop.compress.d2.zip.ModalZipOutputStream;
import org.gbif.hadoop.compress.d2.zip.ZipEntry;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.stream.Collectors;

import lombok.experimental.UtilityClass;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

@UtilityClass
public class D2Utils {

  /** Sets the size, compressed size and crc from the D2CombineInputStream.*/
  public static void setDataFromInputStream(ZipEntry ze, D2CombineInputStream in) {
    ze.setSize(in.getUncompressedLength()); // important to set the sizes and CRC
    ze.setCompressedSize(in.getCompressedLength());
    ze.setCrc(in.getCrc32());
  }

  /** Copies the input path into the ModalZipOutputStream.
   * @return D2CombineInputStream that is close for later use*/
  public static D2CombineInputStream copyToCombinedStream(Path inputPath, FileSystem sourceFS, ModalZipOutputStream zos) throws
    IOException {
    try (D2CombineInputStream in = new D2CombineInputStream(getDataInputStreams(inputPath, sourceFS))) {
      ByteStreams.copy(in, zos);
      return in;
    }
  }

  /** List all files as input streams from the input path.*/
  private static Iterable<InputStream> getDataInputStreams(Path inputPath, FileSystem sourceFS) throws IOException {
    return Arrays.stream(sourceFS.listStatus(inputPath)).sorted().map(fileStatus -> {
      try {
        return sourceFS.open(fileStatus.getPath());
      } catch (IOException ex) {
        throw Throwables.propagate(ex);
      }
    }).collect(Collectors.toList());
  }
}
