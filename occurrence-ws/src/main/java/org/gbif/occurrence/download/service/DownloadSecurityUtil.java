package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.DownloadRequest;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.gbif.registry.security.SecurityContextCheck;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.server.ResponseStatusException;

import static org.gbif.registry.security.UserRoles.ADMIN_ROLE;

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
   * Or, the user is an admin.
   *
   * @throws AccessControlException if no or wrong user is authenticated
   */
  public static void assertLoginMatches(DownloadRequest request, Authentication authentication, Principal principal) {
    if (!principal.getName().equals(request.getCreator()) &&
      !SecurityContextCheck.checkUserInRole(authentication, ADMIN_ROLE)) {
      LOG.warn("Different user authenticated [{}] than download specifies [{}]", principal.getName(),
        request.getCreator());
      throw new ResponseStatusException(HttpStatus.UNAUTHORIZED,
        principal.getName() + " not allowed to create download with creator " + request.getCreator());
    }
  }

  /**
   * Asserts that a user is authenticated, returns the user principal if present.
   */
  public static Principal assertUserAuthenticated(Principal principal) {
    return Optional.ofNullable(principal)
            .orElseThrow(() ->  new ResponseStatusException(HttpStatus.UNAUTHORIZED, "No user authenticated for creating a download"));
  }

  /**
   * Checks if the user can bypass the monthly downloads, i.e. create huge downloads themselves.
   *
   * Used, for example, by the monthly download user (download.gbif.org).
   */
  public static boolean assertMonthlyDownloadBypass(Authentication authentication) {
    if (authentication == null || authentication.getName() == null) {
      return false;
    }

    return SecurityContextCheck.checkUserInRole(authentication, ADMIN_ROLE);
  }
}
