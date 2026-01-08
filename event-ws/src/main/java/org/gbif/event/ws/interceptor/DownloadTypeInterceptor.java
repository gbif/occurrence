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
package org.gbif.event.ws.interceptor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import org.gbif.api.model.occurrence.DownloadRequest;
import org.gbif.api.model.occurrence.DownloadType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.RequestBodyAdvice;

/** An interceptor that sets the download type to Event. */
@SuppressWarnings("NullableProblems")
@ControllerAdvice
public class DownloadTypeInterceptor implements RequestBodyAdvice {

  private static final Logger LOG = LoggerFactory.getLogger(DownloadTypeInterceptor.class);

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public boolean supports(
      MethodParameter methodParameter, Type type, Class<? extends HttpMessageConverter<?>> aClass) {
    return type.getTypeName().equals(DownloadRequest.class.getName())
        && aClass.getName().contains("MappingJackson2HttpMessageConverter");
  }

  @Override
  public HttpInputMessage beforeBodyRead(
      HttpInputMessage httpInputMessage,
      MethodParameter methodParameter,
      Type type,
      Class<? extends HttpMessageConverter<?>> aClass)
      throws IOException {

    byte[] originalBytes =
        org.springframework.util.StreamUtils.copyToByteArray(httpInputMessage.getBody());

    try {
      if (originalBytes.length == 0) {
        return createHttpInputMessageWithBody(originalBytes, httpInputMessage.getHeaders());
      }

      ObjectNode node = (ObjectNode) objectMapper.readTree(originalBytes);
      if (node == null) {
        return createHttpInputMessageWithBody(originalBytes, httpInputMessage.getHeaders());
      }

      node.replace("type", new TextNode(DownloadType.EVENT.name()));

      byte[] modified = objectMapper.writeValueAsBytes(node);
      return createHttpInputMessageWithBody(modified, httpInputMessage.getHeaders());
    } catch (Exception e) {
      LOG.warn("Could not set download type to EVENT in interceptor", e);
      return createHttpInputMessageWithBody(originalBytes, httpInputMessage.getHeaders());
    }
  }

  @Override
  public Object afterBodyRead(
      Object o,
      HttpInputMessage httpInputMessage,
      MethodParameter methodParameter,
      Type type,
      Class<? extends HttpMessageConverter<?>> aClass) {
    return o;
  }

  @Override
  public Object handleEmptyBody(
      Object o,
      HttpInputMessage httpInputMessage,
      MethodParameter methodParameter,
      Type type,
      Class<? extends HttpMessageConverter<?>> aClass) {
    return o;
  }

  private HttpInputMessage createHttpInputMessageWithBody(
      byte[] body, HttpHeaders originalHeaders) {
    HttpHeaders headers = new HttpHeaders();
    if (originalHeaders != null) {
      headers.putAll(originalHeaders);
    }
    headers.setContentLength(body == null ? 0 : body.length);

    final byte[] finalBody = body == null ? new byte[0] : body;
    return new HttpInputMessage() {
      @Override
      public InputStream getBody() {
        return new ByteArrayInputStream(finalBody);
      }

      @Override
      public HttpHeaders getHeaders() {
        return headers;
      }
    };
  }
}
