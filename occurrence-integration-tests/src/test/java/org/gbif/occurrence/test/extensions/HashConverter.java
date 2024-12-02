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
package org.gbif.occurrence.test.extensions;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HashConverter {

  public static String getSha1(String... strings) {
    return getHash("SHA-1", strings);
  }

  @SneakyThrows
  private static String getHash(String algorithm, String... strings) {
    String join = String.join("", strings);
    MessageDigest messageDigest = MessageDigest.getInstance(algorithm);
    byte[] digest = messageDigest.digest(join.getBytes(StandardCharsets.UTF_8));
    StringBuilder hexString = new StringBuilder();
    for (byte hash : digest) {
      String hex = Integer.toHexString(0xff & hash);
      if (hex.length() == 1) {
        hexString.append('0');
      }
      hexString.append(hex);
    }
    return hexString.toString();
  }
}
