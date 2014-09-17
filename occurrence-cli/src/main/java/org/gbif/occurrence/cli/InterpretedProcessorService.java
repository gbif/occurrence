package org.gbif.occurrence.cli;

import org.gbif.common.messaging.DefaultMessagePublisher;
import org.gbif.common.messaging.MessageListener;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.geospatial.LatLng;
import org.gbif.occurrence.persistence.FragmentPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrenceKeyPersistenceServiceImpl;
import org.gbif.occurrence.persistence.OccurrencePersistenceServiceImpl;
import org.gbif.occurrence.persistence.api.FragmentPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrenceKeyPersistenceService;
import org.gbif.occurrence.persistence.api.OccurrencePersistenceService;
import org.gbif.occurrence.persistence.keygen.HBaseLockingKeyService;
import org.gbif.occurrence.processor.InterpretedProcessor;
import org.gbif.occurrence.processor.interpreting.VerbatimOccurrenceInterpreter;
import org.gbif.occurrence.processor.interpreting.util.Wgs84Projection;
import org.gbif.occurrence.processor.messaging.InterpretVerbatimListener;
import org.gbif.occurrence.processor.messaging.VerbatimPersistedListener;
import org.gbif.occurrence.processor.zookeeper.ZookeeperConnector;

import java.util.Collection;
import java.util.Set;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InterpretedProcessorService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(InterpretedProcessorService.class);
  private final ProcessorConfiguration configuration;
  private final Set<MessageListener> listeners = Sets.newHashSet();
  private CuratorFramework curator;
  private final Collection<HTablePool> tablePools = Lists.newArrayList();

  public InterpretedProcessorService(ProcessorConfiguration configuration) {
    this.configuration = configuration;
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
    configuration.ganglia.start();

    curator = configuration.zooKeeper.getCuratorFramework();
    ZookeeperConnector zkConnector = new ZookeeperConnector(curator);

    // we take the max connections from the user and divide over the number of tablepools we need to create
    int individualPoolSize = configuration.hbasePoolSize / 2;
    HTablePool tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);

    tablePool = new HTablePool(HBaseConfiguration.create(), individualPoolSize);
    tablePools.add(tablePool);

    OccurrenceKeyPersistenceService keyService = new OccurrenceKeyPersistenceServiceImpl(
      new HBaseLockingKeyService(configuration.lookupTable, configuration.counterTable, configuration.occTable,
        tablePool));
    FragmentPersistenceService fragmentPersister =
      new FragmentPersistenceServiceImpl(configuration.occTable, tablePool, keyService);

    OccurrencePersistenceService occurrenceService =
      new OccurrencePersistenceServiceImpl(configuration.occTable, tablePool);
    VerbatimOccurrenceInterpreter verbatimInterpreter =
      new VerbatimOccurrenceInterpreter(occurrenceService, zkConnector);
    InterpretedProcessor interpretedProcessor =
      new InterpretedProcessor(fragmentPersister, verbatimInterpreter, occurrenceService,
        new DefaultMessagePublisher(configuration.messaging.getConnectionParameters()), zkConnector);

    MessageListener listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.primaryQueueName, configuration.msgPoolSize,
      new VerbatimPersistedListener(interpretedProcessor));
    listeners.add(listener);

    listener = new MessageListener(configuration.messaging.getConnectionParameters());
    listener.listen(configuration.secondaryQueueName, configuration.msgPoolSize,
      new InterpretVerbatimListener(interpretedProcessor));
    listeners.add(listener);
  }

  @Override
  protected void shutDown() throws Exception {
    for (MessageListener listener : listeners) {
      if (listener != null) {
        listener.close();
      }
    }
    if (curator != null) {
      curator.close();
    }
    for (HTablePool pool : tablePools) {
      if (pool != null) {
        pool.close();
      }
    }
  }
}
