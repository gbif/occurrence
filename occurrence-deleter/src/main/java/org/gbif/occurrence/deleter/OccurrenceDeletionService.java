package org.gbif.occurrence.deleter;

import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.api.model.occurrence.VerbatimOccurrence;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.occurrence.common.identifier.HolyTriplet;
import org.gbif.occurrence.common.identifier.PublisherProvidedUniqueIdentifier;
import org.gbif.occurrence.common.identifier.UniqueIdentifier;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A simple service that can handle the deletion of a single occurrence (including its secondary index entry).
 */
public class OccurrenceDeletionService {

  private static final Logger LOG = LoggerFactory.getLogger(OccurrenceDeletionService.class);

  private final OccurrencePersistenceService occurrenceService;
  private final OccurrenceKeyPersistenceService occurrenceKeyService;

  private final Meter occurrencesDeleted =
    Metrics.newMeter(OccurrenceDeletionService.class, "deletes", "deletes", TimeUnit.SECONDS);

  public OccurrenceDeletionService(OccurrencePersistenceService occurrenceService,
    OccurrenceKeyPersistenceService occurrenceKeyService) {
    this.occurrenceService = checkNotNull(occurrenceService, "occurrenceService can't be null");
    this.occurrenceKeyService = checkNotNull(occurrenceKeyService, "occurrenceKeyService can't be null");
  }

  /**
   * Delete an Occurrence record by key.
   * Optionally, a crawlId can be provided if the occurrenceKey is asked to be deleted for a specific crawl. It
   * ensures the occurrence will not be deleted in case a new crawl updated it.
   *
   * @param occurrenceKey
   * @param crawlId
   *
   * @return the deleted occurrence or null if en error occurred
   */
  public Occurrence deleteOccurrence(int occurrenceKey, @Nullable Integer crawlId) {
    checkArgument(occurrenceKey > 0, "occurrenceKey must be > 0");
    LOG.debug("Deleting occurrence for key [{}]", occurrenceKey);

    // TODO: include dwcOccurrenceId lookup deletion (requires occ id on verbatim object)
    VerbatimOccurrence verbatim = occurrenceService.getVerbatim(occurrenceKey);

    if (verbatim == null) {
      LOG.info("No occurrence for key [{}], ignoring deletion request", occurrenceKey);
      return null;
    }

    // ensure that if a crawlId is provided the occurrence is still at that crawlId otherwise ignore the delete request
    if(crawlId != null && !crawlId.equals(verbatim.getCrawlId())) {
      LOG.info("crawlId [{}] doesn't match the targeted crawlId [{}], ignoring deletion request for key [{}]",
              verbatim.getCrawlId(), crawlId, occurrenceKey);
      return null;
    }

    Set<UniqueIdentifier> lookupsToDelete = Sets.newHashSet();

    // add the 'holy triplet' if any exist
    try {
      if (verbatim.getDatasetKey() != null) {
        final String instCode = verbatim.getVerbatimField(DwcTerm.institutionCode);
        final String collCode = verbatim.getVerbatimField(DwcTerm.collectionCode);
        final String catNum= verbatim.getVerbatimField(DwcTerm.catalogNumber);
        //TODO: retrieve it from somewhere via the persistence layer!
        final String unitQualifier = null;
        lookupsToDelete.add(new HolyTriplet(verbatim.getDatasetKey(), instCode, collCode, catNum, unitQualifier));
      }
    } catch (IllegalArgumentException e) {
      LOG.debug("No valid triplet for occurrenceKey [{}]", occurrenceKey, e);
    }

    // add the occurrenceID if it exists
    String occurrenceID = verbatim.getVerbatimField(DwcTerm.occurrenceID);
    if (!Strings.isNullOrEmpty(occurrenceID)) {
      lookupsToDelete.add(new PublisherProvidedUniqueIdentifier(verbatim.getDatasetKey(), occurrenceID));
    }

    if (lookupsToDelete.isEmpty()) {
      LOG.info("No triplet or occurrenceID found for occurrence [{}] therefore can't delete lookups", occurrenceKey);
    } else {
      occurrenceKeyService.deleteKeyByUniqueIdentifiers(lookupsToDelete);
    }

    // return the deleted occurrence
    Occurrence deleted = occurrenceService.delete(occurrenceKey);
    if (deleted != null) {
      occurrencesDeleted.mark();
    }
    return deleted;
  }
}
