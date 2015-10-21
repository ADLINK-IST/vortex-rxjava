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
import com.prismtech.vortex.rx.RxVortexException;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.DataReaderListener;
import org.omg.dds.sub.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

public final class OnSubscribeToSamplesFromReader<TOPIC> implements Observable.OnSubscribe<Sample<TOPIC>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.RX_VORTEX_LOGGER);

    private final Subject<Sample<TOPIC>, Sample<TOPIC>> subject;
    private final DataReader<TOPIC> dataReader;
    private DataReaderListener<TOPIC> listener;

    private OnSubscribeToSamplesFromReader(DataReader<TOPIC> dataReader) {
        if (dataReader == null) {
            throw new IllegalArgumentException("The ddsSubscriber parameter can not be null.");
        }

        this.dataReader = dataReader;
        subject = PublishSubject.create();
    }

    public static <TOPIC> OnSubscribeToSamplesFromReader<TOPIC> create(DataReader<TOPIC> dataReader) {
        return new OnSubscribeToSamplesFromReader<TOPIC>(dataReader);
    }

    @Override
    public synchronized void call(Subscriber<? super Sample<TOPIC>> subscriber) {
        subject.subscribe(subscriber);

        if (listener == null) {
            try {
                initializeListener();
                dataReader.setListener(listener);
            } catch (RxVortexException e) {
                subscriber.onError(e);
                return;
            }
        }
    }

    private void initializeListener() throws RxVortexException {
        listener = new ListenerForSubject(subject, new SampleGenerator<TOPIC>());
    }

}
