package org.gbif.occurrence.ws;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

@Component
public class TrailingSlashHandlerFilter extends OncePerRequestFilter {

  @Value("${api.url}")
  String apiUrl;

  @Override
  protected void doFilterInternal(
    HttpServletRequest request,
    HttpServletResponse response,
    FilterChain filterChain
  ) throws ServletException, IOException {

    if (request.getRequestURL().toString().endsWith("/")) {
      String newUrl = request.getRequestURL().substring(0, request.getRequestURL().length() - 1);
      StringBuilder url = new StringBuilder(newUrl);
      String queryString = request.getQueryString();
      if (queryString != null) {
        url.append("?").append(queryString);
      }
      response.setStatus(HttpStatus.MOVED_PERMANENTLY.value());
      response.setHeader(HttpHeaders.LOCATION, url.toString());
      return;
    }

    filterChain.doFilter(request, response);
  }
}
