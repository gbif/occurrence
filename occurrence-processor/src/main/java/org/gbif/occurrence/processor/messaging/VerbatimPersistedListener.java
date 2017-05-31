package org.gbif.occurrence.processor.messaging;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.VerbatimPersistedMessage;
import org.gbif.occurrence.processor.FragmentProcessor;
import org.gbif.occurrence.processor.InterpretedProcessor;

import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.slf4j.MDC;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A MessageCallback implementation for VerbatimPersistedMessages. Hands off to the InterpretedProcessor passed in
 * during construction.
 */
@Singleton
public class VerbatimPersistedListener extends AbstractMessageCallback<VerbatimPersistedMessage> {

  private final InterpretedProcessor interpretedProcessor;

  private final Timer processTimer =
    Metrics.newTimer(FragmentProcessor.class, "interp process time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  @Inject
  public VerbatimPersistedListener(InterpretedProcessor interpretedProcessor) {
    checkNotNull(interpretedProcessor, "interpretedProcessor can't be null");
    this.interpretedProcessor = interpretedProcessor;
  }

  @Override
  public void handleMessage(VerbatimPersistedMessage message) {
    final TimerContext context = processTimer.time();
    try {
      MDC.put("datasetKey", message.getDatasetUuid().toString());
      MDC.put("attempt", String.valueOf(message.getAttempt()));
      interpretedProcessor.buildInterpreted(message.getOccurrenceKey(), message.getStatus(), true, message.getAttempt(),
                                            message.getDatasetUuid());
    } finally {
      context.stop();
    }
  }
}
