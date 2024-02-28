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
package org.gbif.occurrence.download.service;

import org.gbif.api.model.occurrence.DownloadRequest;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.server.ResponseStatusException;

import static org.gbif.ws.security.UserRoles.ADMIN_ROLE;

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
    if (principal == null || !principal.getName().equals(request.getCreator()) &&
      !checkUserInRole(authentication, ADMIN_ROLE)) {
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
   * Checks if the user has the given role.
   */
  public static boolean checkUserInRole(Authentication authentication, String... roles) {
    if (authentication == null || authentication.getName() == null) {
      return false;
    }

    if (roles == null || roles.length < 1) {
      return false;
    }

    return Arrays.stream(roles)
      .filter(StringUtils::isNotEmpty)
      .map(SimpleGrantedAuthority::new)
      .anyMatch(role -> authentication.getAuthorities().contains(role));
  }
}
