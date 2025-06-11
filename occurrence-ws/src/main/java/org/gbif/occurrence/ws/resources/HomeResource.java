package org.gbif.occurrence.ws.resources;

import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Trivial controller that redirects root url "/" to swagger pages.
 * Better than seeing the nothing.
 */
@RestController
public class HomeResource {

  @GetMapping("/")
  public void index(HttpServletResponse response) throws Exception {
    response.sendRedirect("/swagger-ui/index.html");
  }
}
