package org.gbif.occurrence.downloads.launcher.resources;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.gbif.occurrence.downloads.launcher.services.LockerService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class DownloadLauncherServiceResourceTest {

  private MockMvc mockMvc;
  private LockerService lockerService;

  @Before
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
