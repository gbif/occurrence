package org.gbif.occurrence.processor.messaging;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.FragmentPersistedMessage;
import org.gbif.occurrence.processor.FragmentProcessor;
import org.gbif.occurrence.processor.VerbatimProcessor;

import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.MDC;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A MessageCallback implementation for FragmentPersistedMessages. Hands off to the VerbatimProcessor passed in during
 * construction.
 */
@Singleton
public class FragmentPersistedListener extends AbstractMessageCallback<FragmentPersistedMessage> {

  private final VerbatimProcessor verbatimProcessor;

  private final Timer processTimer =
    Metrics.newTimer(FragmentProcessor.class, "verb process time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  @Inject
  public FragmentPersistedListener(VerbatimProcessor verbatimProcessor) {
    checkNotNull(verbatimProcessor, "verbatimProcessor can't be null");
    this.verbatimProcessor = verbatimProcessor;
  }

  @Override
  public void handleMessage(FragmentPersistedMessage message) {
    final TimerContext context = processTimer.time();
    try {
      MDC.put("datasetKey", message.getDatasetUuid().toString());
      MDC.put("attempt", String.valueOf(message.getAttempt()));
      verbatimProcessor.buildVerbatim(message.getOccurrenceKey(), message.getStatus(), true, message.getAttempt(),
        message.getDatasetUuid());
    } finally {
      context.stop();
    }
  }
}
