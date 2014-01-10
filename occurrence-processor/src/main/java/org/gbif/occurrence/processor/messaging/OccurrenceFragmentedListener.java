package org.gbif.occurrence.processor.messaging;

import org.gbif.common.messaging.AbstractMessageCallback;
import org.gbif.common.messaging.api.messages.OccurrenceFragmentedMessage;
import org.gbif.occurrence.processor.FragmentProcessor;

import java.util.concurrent.TimeUnit;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A MessageCallback implementation for OccurrenceFragmentedMessage. Hands off to the FragmentProcessor passed in
 * during construction. This is the start of the processing chain.
 */
@Singleton
public class OccurrenceFragmentedListener extends AbstractMessageCallback<OccurrenceFragmentedMessage> {

  private final FragmentProcessor processor;

  private final Timer processTimer =
    Metrics.newTimer(FragmentProcessor.class, "frag process time", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

  @Inject
  public OccurrenceFragmentedListener(FragmentProcessor processor) {
    checkNotNull(processor, "processor can't be null");
    this.processor = processor;
  }

  @Override
  public void handleMessage(OccurrenceFragmentedMessage message) {
    final TimerContext context = processTimer.time();
    try {
      processor.buildFragments(message.getDatasetUuid(), message.getFragment(), message.getSchemaType(),
        message.getEndpointType(), message.getAttempt(), message.getValidationReport());
    } finally {
      context.stop();
    }
  }
}
