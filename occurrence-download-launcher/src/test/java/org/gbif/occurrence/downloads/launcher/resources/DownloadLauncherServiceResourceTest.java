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
package org.gbif.occurrence.downloads.launcher.resources;

import org.gbif.occurrence.downloads.launcher.services.LockerService;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class DownloadLauncherServiceResourceTest {

  private MockMvc mockMvc;
  private LockerService lockerService;

  @BeforeEach
  public void setUp() {
    lockerService = Mockito.mock(LockerService.class);
    mockMvc =
        MockMvcBuilders.standaloneSetup(new DownloadLauncherServiceResource(lockerService)).build();
  }

  @Test
  public void testUnlockAll() throws Exception {
    mockMvc.perform(MockMvcRequestBuilders.delete("/unlock")).andExpect(status().isOk());

    Mockito.verify(lockerService).unlockAll();
  }

  @Test
  public void testUnlock() throws Exception {
    String downloadKey = "test-key";
    mockMvc
        .perform(MockMvcRequestBuilders.delete("/unlock/" + downloadKey))
        .andExpect(status().isOk());

    Mockito.verify(lockerService).unlock(downloadKey);
  }
}
