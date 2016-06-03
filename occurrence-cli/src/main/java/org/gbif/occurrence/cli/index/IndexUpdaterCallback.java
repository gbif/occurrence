package org.gbif.occurrence.cli.index;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Callback handler that processes messages for updates and insertions on the Occurrence Index.
 */
class IndexUpdaterCallback extends AbstractMessageCallback<OccurrenceMutatedMessage> {

  private static final Logger LOG = LoggerFactory.getLogger(IndexUpdaterCallback.class);
  private final Counter messageCount = Metrics.newCounter(getClass(), "messageCount");
  private final Counter newOccurrencesCount = Metrics.newCounter(getClass(), "newIndexedOccurrencesCount");
  private final Counter updatedOccurrencesCount = Metrics.newCounter(getClass(), "updatedIndexedOccurrencesCount");
  private final Counter deletedOccurrencesCount = Metrics.newCounter(getClass(), "deletedIndexedOccurrencesCount");
  private final Timer writeTimer = Metrics.newTimer(getClass(), "occurrenceIndexWrites", TimeUnit.MILLISECONDS,
                                                    TimeUnit.SECONDS);
  private final SolrOccurrenceWriter solrOccurrenceWriter;

  /**
   * Default constructor.
   */
  public IndexUpdaterCallback(SolrOccurrenceWriter solrOccurrenceWriter) {
    this.solrOccurrenceWriter = solrOccurrenceWriter;
  }

  @Override
  public void handleMessage(OccurrenceMutatedMessage message) {
    LOG.debug("Handling [{}] occurrence", message.getStatus());
    messageCount.inc();
    TimerContext context = writeTimer.time();
    try {
      switch (message.getStatus()) {
        case NEW:
          // create occurrence
          solrOccurrenceWriter.update(message.getNewOccurrence());
          newOccurrencesCount.inc();
          break;
        case UPDATED:
          // update occurrence
          solrOccurrenceWriter.update(message.getNewOccurrence());
          updatedOccurrencesCount.inc();
          break;
        case DELETED:
          // delete occurrence
          solrOccurrenceWriter.delete(message.getOldOccurrence());
          deletedOccurrencesCount.inc();
          break;
        case UNCHANGED:
          break;
      }
    } catch (Exception e) {
      LOG.error("Error while updating occurrence index for [{}], error [{}]", message.getStatus(), e);
    } finally {
      context.stop();
    }
  }
}
