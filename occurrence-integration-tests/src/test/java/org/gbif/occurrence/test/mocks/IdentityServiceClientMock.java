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

import org.gbif.api.model.common.GbifUser;
import org.gbif.ws.remoteauth.IdentityServiceClient;
import org.gbif.ws.remoteauth.LoggedUser;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class IdentityServiceClientMock implements IdentityServiceClient {

  private final LoggedUser testUser;
  private final String testCredentials;

  @Override
  public GbifUser get(String userName) {
    return testUser.toGbifUser();
  }

  @Override
  public LoggedUser login(String credentials) {
    return testUser;
  }

  @Override
  public GbifUser authenticate(String userName, String password) {
    return testUser.toGbifUser();
  }
}
