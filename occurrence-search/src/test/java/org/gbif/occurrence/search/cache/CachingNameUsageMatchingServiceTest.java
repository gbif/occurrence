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
package org.gbif.occurrence.search.cache;

import org.gbif.rest.client.species.Metadata;
import org.gbif.rest.client.species.NameUsageMatchingService;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the CachingNameUsageMatchingService. */
public class CachingNameUsageMatchingServiceTest {

  @Test
  public void metadataIsCachedTest() {
    NameUsageMatchingService delegate = mock(NameUsageMatchingService.class);
    Metadata metadata = mock(Metadata.class);
    when(delegate.getMetadata("a")).thenReturn(metadata);

    CachingNameUsageMatchingService service = new CachingNameUsageMatchingService(delegate);

    assertSame(metadata, service.getMetadata("a"));
    assertSame(metadata, service.getMetadata("a"));
    verify(delegate, times(1)).getMetadata("a");
  }

  @Test
  public void nullMetadataIsCachedTest() {
    NameUsageMatchingService delegate = mock(NameUsageMatchingService.class);

    CachingNameUsageMatchingService service = new CachingNameUsageMatchingService(delegate);

    assertNull(service.getMetadata("a"));
    assertNull(service.getMetadata("a"));
    verify(delegate, times(1)).getMetadata("a");
  }

  @Test
  public void loaderExceptionIsUnwrappedAndNotCachedTest() {
    NameUsageMatchingService delegate = mock(NameUsageMatchingService.class);
    IllegalArgumentException error = new IllegalArgumentException("unknown checklist");
    when(delegate.getMetadata("a")).thenThrow(error);

    CachingNameUsageMatchingService service = new CachingNameUsageMatchingService(delegate);

    assertSame(error, assertThrows(IllegalArgumentException.class, () -> service.getMetadata("a")));
    assertSame(error, assertThrows(IllegalArgumentException.class, () -> service.getMetadata("a")));
    verify(delegate, times(2)).getMetadata("a");
  }
}
