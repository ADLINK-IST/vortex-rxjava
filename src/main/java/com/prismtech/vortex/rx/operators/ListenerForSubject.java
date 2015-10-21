/**
 * PrismTech licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License and with the PrismTech Vortex product. You may obtain a copy of the
 * License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License and README for the specific language governing permissions and
 * limitations under the License.
 */
package com.prismtech.vortex.rx.operators;

import com.prismtech.vortex.rx.Config;
import org.omg.dds.core.event.*;
import org.omg.dds.sub.DataReaderListener;
import org.omg.dds.sub.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Func1;
import rx.subjects.Subject;

public class ListenerForSubject<TOPIC,STREAM> implements DataReaderListener<TOPIC> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.RX_VORTEX_LOGGER);

    private final Subject<STREAM, STREAM> subject;
    private final Func1<Sample<TOPIC>, STREAM> gen;

    public ListenerForSubject(Subject<STREAM, STREAM> subject,
                              Func1<Sample<TOPIC>, STREAM> gen) {
        if(subject == null) {
            throw new NullPointerException("The subject can not be null.");
        }

        if(gen == null) {
            throw new NullPointerException("The gen can not be null.");
        }

        this.subject = subject;
        this.gen = gen;
    }

    @Override
    public void onRequestedDeadlineMissed(RequestedDeadlineMissedEvent<TOPIC> event) {
        LOGGER.warn(String.format("Requested deadline missed %d total_count.",
                event.getStatus().getTotalCount()));
    }

    @Override
    public void onRequestedIncompatibleQos(RequestedIncompatibleQosEvent<TOPIC> event) {
        LOGGER.warn(String.format("Requested incompatible QoS %d total_count.",
                event.getStatus().getTotalCount()));
    }

    @Override
    public void onSampleRejected(SampleRejectedEvent<TOPIC> event) {
        LOGGER.warn(String.format("Sample rejected because %s.",
                event.getStatus().getLastReason().toString()));
    }

    @Override
    public void onLivelinessChanged(LivelinessChangedEvent<TOPIC> event) {
        LOGGER.warn(String.format("Liveliness changed. %d is now alive",
                event.getStatus().getAliveCount()));
    }

    @Override
    public void onDataAvailable(DataAvailableEvent<TOPIC> event) {
        LOGGER.debug("More data is available");

            final Sample.Iterator<TOPIC> datum = event.getSource().take();

            while (datum.hasNext()) {
                Sample<TOPIC> sample = datum.next();
                final STREAM data = gen.call(sample);
                if(data != null) {
                    subject.onNext(data);
                }
            }
    }

    @Override
    public void onSubscriptionMatched(SubscriptionMatchedEvent<TOPIC> event) {
        LOGGER.warn(String.format("Subscription changed. %d current count.",
                event.getStatus().getCurrentCount()));
    }

    @Override
    public void onSampleLost(SampleLostEvent<TOPIC> event) {
        LOGGER.warn(String.format("Samples lost %d.",
                event.getStatus().getTotalCount()));
    }
}
