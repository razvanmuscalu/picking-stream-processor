package tech.picnic.assignment.impl;

import com.google.auto.service.AutoService;
import tech.picnic.assignment.api.EventProcessorFactory;
import tech.picnic.assignment.api.StreamProcessor;

import java.time.Duration;

@AutoService(EventProcessorFactory.class)
public final class PickingEventProcessorFactory implements EventProcessorFactory {
    @Override
    public StreamProcessor createProcessor(int maxEvents, Duration maxTime) {
        return new PickingStreamProcessor(new SourceReader(maxEvents, maxTime));
    }
}
