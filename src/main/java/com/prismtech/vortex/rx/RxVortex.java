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
package com.prismtech.vortex.rx;

import com.prismtech.vortex.rx.operators.OnSubscribeFromParticipant;
import com.prismtech.vortex.rx.operators.OnSubscribeFromReader;
import com.prismtech.vortex.rx.operators.OnSubscribeFromReaderWaitset;
import com.prismtech.vortex.rx.operators.OnSubscribeToSamplesFromReader;
import org.omg.dds.core.Duration;
import org.omg.dds.core.ServiceEnvironment;
import org.omg.dds.core.policy.QosPolicy;
import org.omg.dds.domain.DomainParticipant;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Action0;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class RxVortex {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.RX_VORTEX_LOGGER);

    /**
     * Create a new observable for the provided {@link DataReader} <code>dr</code>.
     *
     * The observable will be a stream of the data contained in each sample.
     *
     * It is backed by a wait set rather than a Listener.
     *
     * @param dr
     *      the data reader being observed
     * @param <TOPIC>
     *      the type of dat
     * @return
     */
    public static <TOPIC> Observable<TOPIC> fromDataReaderWS(final DataReader<TOPIC> dr) {
        ServiceEnvironment env = ServiceEnvironment.createInstance(
                RxVortex.class.getClassLoader()
        );
        Observable<TOPIC> subject = Observable.create(new OnSubscribeFromReaderWaitset<TOPIC>(dr, Duration.newDuration(60, TimeUnit.SECONDS, env)));
        return subject;
    }

    /**
     * Create a new observable for the provided {@link DataReader} <code>dr</code>.
     *
     * The observable will be a stream of the data contained in each sample.
     *
     * @param dr
     *      the data reader being observed
     * @param <TOPIC>
     *      the type of data
     * @return
     */
    public static <TOPIC> Observable<TOPIC> fromDataReader(final DataReader<TOPIC> dr) {
        Observable<TOPIC> subject = Observable.create(OnSubscribeFromReader.create(dr));

        return subject;
    }

    /**
     * Create a new observable for the provided {@link DataReader} <code>dr</code>.
     *
     * The observable will be a stream of the samples.
     *
     * @param dr
     *      the data reader being observed
     * @param <TOPIC>
     *      the type of data
     * @return
     */
    public static <TOPIC> Observable<Sample<TOPIC>> samplesFromDataReader(final DataReader<TOPIC> dr) {
        Observable<Sample<TOPIC>> subject = Observable.create(OnSubscribeToSamplesFromReader.create(dr));

        return subject;
    }

    /**
     * Create a new observable for the <code>topicName</code> using the provided {@link DomainParticipant}.
     *
     * The observable will be a stream of the data contained in each sample.
     *
     * @param participant
     *          the participant to create the observable from
     * @param topicName
     *          the name of the topic that is being observed
     * @param topicType
     *          the type of the topic that is being observed
     * @param partitions
     *          a possibly empty list of the partitions in which the topic is being observed
     * @param qos
     *          a possibly empty of {@link QosPolicy.ForDataReader}
     * @param <TOPIC>
     *          the type of data
     * @return
     *          the observable
     */
    public static <TOPIC> Observable<TOPIC> fromParticipant(final DomainParticipant participant,
                                                            final String topicName,
                                                            final Class<TOPIC> topicType,
                                                            final List<String> partitions,
                                                            final QosPolicy.ForDataReader... qos) {
        final OnSubscribeFromParticipant<TOPIC> onSubscribe = OnSubscribeFromParticipant.create(participant, topicName, topicType, partitions);
        final Observable<TOPIC> observable = Observable.create(onSubscribe);
        observable.doOnTerminate(new Action0() {
            @Override
            public void call() {
                onSubscribe.cleanUp();
            }
        });

        return observable;
    }
}
