package org.gbif.occurrence.ws.client.mock;

import org.gbif.api.model.common.paging.Pageable;
import org.gbif.api.model.common.paging.PagingResponse;
import org.gbif.api.model.occurrence.Download;
import org.gbif.api.model.registry.DatasetOccurrenceDownloadUsage;
import org.gbif.api.service.registry.OccurrenceDownloadService;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

public class OccurrenceDownloadMockServices implements OccurrenceDownloadService {

  @Override
  public void create(@NotNull Download download) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Download get(@NotNull String s) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public PagingResponse<Download> list(
    @Nullable Pageable pageable
  ) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public PagingResponse<Download> listByUser(
    @NotNull String s, @Nullable Pageable pageable
  ) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public void update(@NotNull Download download) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public PagingResponse<DatasetOccurrenceDownloadUsage> listDatasetUsages(
    @NotNull String s, @Nullable Pageable pageable
  ) {
    // TODO: Write implementation
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
