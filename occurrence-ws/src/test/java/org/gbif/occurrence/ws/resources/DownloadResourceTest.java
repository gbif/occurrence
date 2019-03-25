package org.gbif.occurrence.ws.resources;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import org.gbif.api.model.common.GbifUserPrincipal;
import org.gbif.api.model.occurrence.DownloadFormat;
import org.gbif.api.model.occurrence.PredicateDownloadRequest;
import org.gbif.api.model.occurrence.predicate.EqualsPredicate;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.service.occurrence.DownloadRequestService;
import org.gbif.api.service.registry.OccurrenceDownloadService;
import org.gbif.occurrence.download.service.CallbackService;
import org.gbif.ws.security.NotAllowedException;
import org.junit.Test;

public class DownloadResourceTest {

  private static final String JOB_ID = "FOO";
  private static final String USER = "testuser";
  private static final String STATUS = "SUCCEEDED";

  private DownloadResource resource;
  private PredicateDownloadRequest dl;
  private SecurityContext sec;

  @Test
  public void testCallback() {
    prepareMocks(USER);
    Response response = resource.oozieCallback(JOB_ID, STATUS);
    assertThat(response.getStatus(), equalTo(Response.Status.OK.getStatusCode()));
  }

  @Test
  public void testStartDownload() {
    prepareMocks(USER);
    String jobId = resource.startDownload(dl, sec);
    assertThat(jobId, equalTo(JOB_ID));
  }

  @Test(expected = NotAllowedException.class)
  public void testStartDownloadNotAuthenticated() {
    prepareMocks("foo");
    resource.startDownload(dl, sec);
  }

  private void prepareMocks(String user) {
    CallbackService callbackService = mock(CallbackService.class);
    DownloadRequestService service = mock(DownloadRequestService.class);
    OccurrenceDownloadService downloadService = mock(OccurrenceDownloadService.class);
    sec = mock(SecurityContext.class);
    GbifUserPrincipal userP = mock(GbifUserPrincipal.class);
    when(userP.getName()).thenReturn(user);
    when(sec.getUserPrincipal()).thenReturn(userP);

    resource = new DownloadResource(service, callbackService, downloadService);
    dl = new PredicateDownloadRequest(new EqualsPredicate(OccurrenceSearchParameter.TAXON_KEY, "1"), USER, null, true,
      DownloadFormat.DWCA);
    when(service.create(dl)).thenReturn(JOB_ID);
  }

}
