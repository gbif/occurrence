package org.gbif.occurrence.processor.messaging;

import org.gbif.api.vocabulary.OccurrencePersistenceStatus;
import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.ParseFragmentMessage;
import org.gbif.occurrence.processor.FragmentProcessor;
import org.gbif.occurrence.processor.VerbatimProcessor;

import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import static com.google.common.base.Preconditions.checkNotNull;

public class ParseFragmentListener extends AbstractMessageCallback<ParseFragmentMessage> {

  private final VerbatimProcessor verbatimProcessor;

  private final Timer processTimer =
    Metrics.newTimer(FragmentProcessor.class, "verb process time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  @Inject
  public ParseFragmentListener(VerbatimProcessor verbatimProcessor) {
    checkNotNull(verbatimProcessor, "verbatimProcessor can't be null");
    this.verbatimProcessor = verbatimProcessor;
  }

  @Override
  public void handleMessage(ParseFragmentMessage message) {
    final TimerContext context = processTimer.time();
    try {
      verbatimProcessor
        .buildVerbatim(message.getOccurrenceKey(), OccurrencePersistenceStatus.UPDATED, false, null, null);
    } finally {
      context.stop();
    }
  }
}
