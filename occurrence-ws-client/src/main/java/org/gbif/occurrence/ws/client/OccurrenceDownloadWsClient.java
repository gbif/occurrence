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
package org.gbif.occurrence.ws.client;




import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;


/**
 * Client-side implementation to the occurrence download service.
 */
@RequestMapping(
  value = Constants.OCCURRENCE_DOWNLOAD_PATH,
  produces = MediaType.APPLICATION_JSON_VALUE
)
public interface OccurrenceDownloadWsClient extends BaseDownloadWsClient {

}
