package com.landoop.lenses.topology.client.kafka.spark;

import com.landoop.lenses.topology.client.TopologyClient;
import com.landoop.lenses.topology.client.metrics.Metrics;
import com.landoop.lenses.topology.client.metrics.MetricsBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

public class MetricsPublishTask2 implements Runnable {

    private final String appName;
    private final Duration publishInterval;
    private final TopologyClient client;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public MetricsPublishTask2(TopologyClient client, String appName, Duration publishInterval) {
        this.client = client;
        this.appName = appName;
        this.publishInterval = publishInterval;
    }

    @Override
    public void run() {
        while (running.get()) {
            try {
                Thread.sleep(publishInterval.toMillis());
                publish();
                if (Thread.interrupted())
                    throw new InterruptedException();
            } catch (InterruptedException e) {
                running.set(false);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void publish() throws IOException {
        for (MetricsBuilder producer : client.getMetricProducers()) {
            Metrics metrics = producer.build(appName);
            client.send(metrics);
        }
        client.flush();
    }
}
