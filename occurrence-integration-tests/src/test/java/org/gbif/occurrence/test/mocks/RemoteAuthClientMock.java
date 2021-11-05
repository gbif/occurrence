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
package org.gbif.occurrence.test.mocks;

import org.gbif.ws.remoteauth.LoggedUser;
import org.gbif.ws.remoteauth.RemoteAuthClient;

import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;

@Data
@Builder
public class RemoteAuthClientMock implements RemoteAuthClient {

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

  private final LoggedUser testUser;

  @SneakyThrows
  @Override
  public ResponseEntity<String> remoteAuth(String s, HttpHeaders httpHeaders) {
    return ResponseEntity.ok(OBJECT_MAPPER.writeValueAsString(testUser));
  }
}
