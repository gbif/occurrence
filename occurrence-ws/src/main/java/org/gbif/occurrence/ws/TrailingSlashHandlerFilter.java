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
    String requestUri = request.getRequestURI();

    if (requestUri.endsWith("/")) {
      String newUrl = apiUrl + requestUri.substring(0, requestUri.length() - 1);
      response.setStatus(HttpStatus.MOVED_PERMANENTLY.value());
      response.setHeader(HttpHeaders.LOCATION, newUrl);
      return;
    }

    filterChain.doFilter(request, response);
  }
}
