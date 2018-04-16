package org.gbif.occurrence.cli.index;

import org.gbif.common.messaging.ConnectionParameters;
import org.gbif.common.messaging.MessageListener;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Message listener that uses a IndexUpdaterCallback (or multiple) and are close when this is instance is closed too.
 */
public class IndexMessageListener extends MessageListener {

    List<IndexUpdaterCallback> callbacks = new ArrayList<>();

    public IndexMessageListener(ConnectionParameters connectionParameters) throws IOException {
        super(connectionParameters);
    }

    public void listen(String queue, int numberOfThreads, IndexUpdaterCallback callback) throws IOException {
        super.listen(queue, numberOfThreads, callback);
        callbacks.add(callback);
    }

    @Override
    public void close() {
      super.close();
      callbacks.forEach(IndexUpdaterCallback::close);
    }
}
