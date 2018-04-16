package org.gbif.occurrence.cli.index;

import org.apache.solr.client.solrj.SolrServerException;
import org.gbif.api.model.occurrence.Occurrence;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.OccurrenceMutatedMessage;
import org.gbif.occurrence.search.writer.SolrOccurrenceWriter;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
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
class IndexUpdaterCallback extends AbstractMessageCallback<OccurrenceMutatedMessage>  implements Closeable {

  private static final int UPDATE_BATCH_SIZE = 1000;

  private static final Logger LOG = LoggerFactory.getLogger(IndexUpdaterCallback.class);
  private final Counter messageCount = Metrics.newCounter(getClass(), "messageCount");
  private final Counter newOccurrencesCount = Metrics.newCounter(getClass(), "newIndexedOccurrencesCount");
  private final Counter updatedOccurrencesCount = Metrics.newCounter(getClass(), "updatedIndexedOccurrencesCount");
  private final Counter deletedOccurrencesCount = Metrics.newCounter(getClass(), "deletedIndexedOccurrencesCount");
  private final Timer writeTimer = Metrics.newTimer(getClass(), "occurrenceIndexWrites", TimeUnit.MILLISECONDS,
                                                    TimeUnit.SECONDS);

  private final Duration updateWithin;

  private final SolrOccurrenceWriter solrOccurrenceWriter;

  private final List<Occurrence> updateBatch;

  private LocalDate lastUpdate = LocalDate.now();

  private final ScheduledExecutorService updateTimer = Executors.newSingleThreadScheduledExecutor();

  private void atomicAddOrUpdate() throws IOException, SolrServerException {
    addOrUpdate(updateBatch.size() >= UPDATE_BATCH_SIZE
            || LocalDate.now().minus(updateWithin).compareTo(lastUpdate) >= 0);
  }

  /**
   * Flushes all the updates/creates into Solr.
   */
  private void addOrUpdate(boolean onCondition) throws IOException, SolrServerException {
      synchronized (updateBatch) {
        if(onCondition) {
            try {
                solrOccurrenceWriter.update(updateBatch);
            } finally {
                updateBatch.clear();
                lastUpdate = LocalDate.now();
            }
        }
      }
  }

  /**
   * Default constructor.
   */
  public IndexUpdaterCallback(SolrOccurrenceWriter solrOccurrenceWriter, int solrUpdateBatchSize,
                              long solrUpdateWithinMs) {
    this.solrOccurrenceWriter = solrOccurrenceWriter;
    updateBatch = Collections.synchronizedList(new ArrayList<>(solrUpdateBatchSize));
    updateWithin = Duration.ofMillis(solrUpdateWithinMs);
    updateTimer.scheduleWithFixedDelay(() -> {
                try {
                  atomicAddOrUpdate();
                } catch (Exception ex){
                  throw new RuntimeException(ex);
                }
            }, solrUpdateWithinMs, solrUpdateWithinMs, TimeUnit.MILLISECONDS);
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
          updateBatch.add(message.getNewOccurrence());
          atomicAddOrUpdate();
          newOccurrencesCount.inc();
          break;
        case UPDATED:
          // update occurrence
          updateBatch.add(message.getNewOccurrence());
          atomicAddOrUpdate();
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

  /**
   *  Tries an update and stop the timer.
   */
  @Override
  public void close() {
    try {
      addOrUpdate(true);
    } catch (Exception e) {
      LOG.error("Error closing callback", e);
    }
    updateTimer.shutdownNow();
  }
}
