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
package org.gbif.occurrence.download.resource;

import org.gbif.occurrence.download.service.DownloadRequestsValidator;
import org.gbif.ws.server.GbifHttpServletRequestWrapper;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.http.HttpStatus;

import lombok.SneakyThrows;

/**
 * Simple validation filter for download requests.
 * Performs a validation against a Json schema.
 */
public class DownloadRequestValidationFilter implements Filter {

  private final DownloadRequestsValidator downloadRequestsValidator = new DownloadRequestsValidator();

  @Override
  public void doFilter(
    ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain
  ) throws IOException, ServletException {
    HttpServletRequest request = (HttpServletRequest) servletRequest;
    if ("POST".equalsIgnoreCase(request.getMethod())) {
      GbifHttpServletRequestWrapper gbifHttpServletRequestWrapper = new GbifHttpServletRequestWrapper(request, true);
      if (isDownloadRequestValid(gbifHttpServletRequestWrapper.getContent(), (HttpServletResponse) servletResponse)) {
        filterChain.doFilter(gbifHttpServletRequestWrapper, servletResponse);
      }
    } else {
      filterChain.doFilter(servletRequest, servletResponse);
    }
  }

  /**
   * Is the download request Json object valid.
   */
  private boolean isDownloadRequestValid(String jsonDownloadRequest, HttpServletResponse response) {
    try {
      downloadRequestsValidator.validate(jsonDownloadRequest);
      return true;
    } catch (Exception exception) {
      setErrorContent(response, exception);
      return false;
    }
  }

  /**
   * Sets the response error content.
   */
  @SneakyThrows
  private void setErrorContent(HttpServletResponse response, Exception exception) {
    response.setStatus(HttpStatus.BAD_REQUEST.value());
    String responseBody = String.join("\n", exception.getMessage());
    response.setContentLength(responseBody.length());
    response.getOutputStream().write(responseBody.getBytes());
  }
}
