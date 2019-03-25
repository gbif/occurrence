package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.ws.security.NotAllowedException;
import org.gbif.ws.security.NotAuthenticatedException;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Optional;
import javax.ws.rs.core.SecurityContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Common security checks used for occurrence downloads.
 */
public class DownloadSecurityUtil {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadSecurityUtil.class);

  /**
   * Private constructor.
   */
  private DownloadSecurityUtil() {
    //empty constructor
  }

  /**
   * Checks that a user is authenticated and the same user is the creator of the download.
   *
   * @throws AccessControlException if no or wrong user is authenticated
   */
  public static void assertLoginMatches(DownloadRequest request, SecurityContext security) {
    // assert authenticated user is the same as in download
    assertLoginMatches(request, assertUserAuthenticated(security));
  }

  /**
   * Checks that the user principal.name is the creator of the download.
   *
   * @throws AccessControlException if no or wrong user is authenticated
   */
  public static void assertLoginMatches(DownloadRequest request, Principal principal) {
    if (!principal.getName().equals(request.getCreator())) {
      LOG.warn("Different user authenticated [{}] than download specifies [{}]", principal.getName(),
        request.getCreator());
      throw new NotAllowedException(principal.getName() + " not allowed to create download with creator "
        + request.getCreator());
    }
  }

  /**
   * Asserts that a user is authenticated, returns the user principal if present.
   */
  public static Principal assertUserAuthenticated(SecurityContext securityContext) {
    // assert authenticated user is the same as in download
    return Optional.ofNullable(securityContext.getUserPrincipal())
            .orElseThrow(() -> new NotAuthenticatedException("No user authenticated for creating a download"));
  }
}
