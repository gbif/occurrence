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
package org.gbif.occurrence.trino;

import java.util.Optional;
import java.util.Properties;

import javax.annotation.Nullable;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ConnectionConfiguration {

  private final String user;

  @Nullable
  private final String password;

  @Builder.Default
  private final Boolean ssl = true;

  @Builder.Default
  private final String sslVerification = "NONE";

  //Connection URL, example: jdbc:trino://localhost:8443/gbif/downloads
  private final String url;

  public Properties toProperties() {
    Properties properties = new Properties();
    properties.setProperty("user", user);
    Optional.ofNullable(password).ifPresent(pwd -> properties.setProperty("password", pwd));
    properties.setProperty("SSL", ssl.toString());
    properties.setProperty("SSLVerification", sslVerification.toUpperCase());
    return properties;
  }
}
