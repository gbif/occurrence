package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.DownloadRequest;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

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
   * Checks that the user principal.name is the creator of the download.
   *
   * @throws AccessControlException if no or wrong user is authenticated
   */
  public static void assertLoginMatches(DownloadRequest request, Principal principal) {
    if (!principal.getName().equals(request.getCreator())) {
      LOG.warn("Different user authenticated [{}] than download specifies [{}]", principal.getName(),
        request.getCreator());
      throw new ResponseStatusException(HttpStatus.METHOD_NOT_ALLOWED, principal.getName() + " not allowed to create download with creator "
                                                            + request.getCreator());
    }
  }

  /**
   * Asserts that a user is authenticated, returns the user principal if present.
   */
  public static Principal assertUserAuthenticated(Principal principal) {
    // assert authenticated user is the same as in download
    return Optional.ofNullable(principal)
            .orElseThrow(() ->  new ResponseStatusException(HttpStatus.UNAUTHORIZED, "No user authenticated for creating a download"));
  }
}
