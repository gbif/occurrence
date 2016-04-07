package org.gbif.occurrence.cli;

import org.gbif.common.messaging.MessageListener;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.occurrence.processor.InterpretedProcessor;
import org.gbif.occurrence.processor.guice.OccurrenceProcessorModule;
import org.gbif.occurrence.processor.interpreting.util.Wgs84Projection;
import org.gbif.occurrence.processor.messaging.InterpretVerbatimListener;
import org.gbif.occurrence.processor.messaging.VerbatimPersistedListener;

import java.util.Set;

import com.beust.jcommander.internal.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterpretedProcessorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedProcessorService.class);
  private final ProcessorCliConfiguration cfg;
  private final Set<MessageListener> listeners = Sets.newHashSet();

  public InterpretedProcessorService(ProcessorCliConfiguration configuration) {
    this.cfg = configuration;
  }

  /**
   * Simply tries the WGS84 reprojection one time to load all geotools plugins and check if its all fine.
   * We have seen classpath issues and spend far too much time on this, so best to keep this little test in the code.
   * Without it we rely on messages coming in with actual datums to kickoff the geotools init routines that caused
   * trouble.
   */
  private void testReprojection() {
    LOG.info("Testing geodetic datum reprojection on startup...");
    ParseResult<LatLng> res = Wgs84Projection.reproject(2, 2, "NAD27");
    LOG.info("2/2 reprojected from NDA27 to WGS84: " + res);
  }

  @Override
  protected void startUp() throws Exception {
    Injector inj = Guice.createInjector(new OccurrenceProcessorModule(cfg));

    cfg.ganglia.start();

    MessageListener listener = new MessageListener(cfg.messaging.getConnectionParameters());
    listener.listen(cfg.primaryQueueName, cfg.msgPoolSize,
                    new VerbatimPersistedListener(inj.getInstance(InterpretedProcessor.class)));
    listeners.add(listener);

    listener = new MessageListener(cfg.messaging.getConnectionParameters());
    listener.listen(cfg.secondaryQueueName, cfg.msgPoolSize,
                    new InterpretVerbatimListener(inj.getInstance(InterpretedProcessor.class)));
    listeners.add(listener);
  }

  @Override
  protected void shutDown() throws Exception {
    for (MessageListener listener : listeners) {
      if (listener != null) {
        listener.close();
      }
    }
  }
}
