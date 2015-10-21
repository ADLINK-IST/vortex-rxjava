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
import org.omg.dds.core.policy.PolicyFactory;
import org.omg.dds.core.policy.QosPolicy;
import org.omg.dds.domain.DomainParticipant;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.DataReaderListener;
import org.omg.dds.sub.Sample;
import org.omg.dds.sub.SubscriberQos;
import org.omg.dds.topic.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.Collections;
import java.util.List;

public final class OnSubscribeFromParticipant<TOPIC> implements Observable.OnSubscribe<TOPIC> {
    private static final Logger LOGGER = LoggerFactory.getLogger(Config.RX_VORTEX_LOGGER);

    private final DomainParticipant participant;
    private final String topicName;
    private final Class<TOPIC> topicType;
    private final Subject<TOPIC, TOPIC> subject;
    private final List<String> partitions;
    private DataReader<TOPIC> dataReader;
    private org.omg.dds.sub.Subscriber ddsSubscriber;
    private DataReaderListener<TOPIC> listener;

    private OnSubscribeFromParticipant(DomainParticipant participant,
                                       String topicName,
                                       Class<TOPIC> topicType,
                                       final List<String> partitions, QosPolicy.ForDataReader... qos) {
        if (participant == null) {
            throw new IllegalArgumentException("The participant parameter can not be null.");
        }

        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("The topic name parameter can not be null or empty.");
        }

        if (topicType == null) {
            throw new IllegalArgumentException("The topic type parameter can not be null.");
        }

        this.participant = participant;
        this.topicName = topicName;
        this.topicType = topicType;
        this.partitions = partitions;
        subject = PublishSubject.create();
    }

    public static <TOPIC> OnSubscribeFromParticipant<TOPIC> create(DomainParticipant participant, String topicName, Class<TOPIC> topicType) {
        return create(participant, topicName, topicType, Collections.<String>emptyList());
    }

    public static <TOPIC> OnSubscribeFromParticipant<TOPIC> create(final DomainParticipant participant,
                                                                   final String topicName,
                                                                   final Class<TOPIC> topicType,
                                                                   final List<String> partitions,
                                                                   final QosPolicy.ForDataReader... qos) {
        return new OnSubscribeFromParticipant<TOPIC>(participant, topicName, topicType, partitions,qos);
    }

    @Override
    public synchronized void call(Subscriber<? super TOPIC> subscriber) {
        subject.subscribe(subscriber);
        if (dataReader == null) {
            try {
                initializeDataReader();
            } catch (RxVortexException e) {
                subscriber.onError(e);
                return;
            }
        }

        if (listener == null) {
            try {
                initializeListener();
            } catch (RxVortexException e) {
                subscriber.onError(e);
                return;
            }
        }

        dataReader.setListener(listener);
    }

    public synchronized void cleanUp() {
        dataReader.setListener(null);

        if(dataReader != null) {
            dataReader.close();
        }

        if(ddsSubscriber != null) {
            ddsSubscriber.close();
        }
    }

    private void initializeDataReader() throws RxVortexException {
        final PolicyFactory pf =
                PolicyFactory.getPolicyFactory(participant.getEnvironment());

        if (ddsSubscriber == null) {
            final SubscriberQos subQos = participant.getDefaultSubscriberQos().withPolicy(
                    pf.Partition().withName(partitions)
            );
            ddsSubscriber = participant.createSubscriber(subQos);

            if (ddsSubscriber == null) {
                throw new RxVortexException("Unable to create a new Subscriber.");
            }
        }

        if (dataReader == null) {
            Topic<TOPIC> topicToRead = participant.createTopic(topicName, topicType);

            if (topicToRead == null) {
                throw new RxVortexException("Unable to create the topic.");
            }

            dataReader = ddsSubscriber.createDataReader(topicToRead);

            if (dataReader == null) {
                throw new RxVortexException(("Unable to create a new data reader"));
            }
        }
    }

    private void initializeListener() throws RxVortexException {
        listener = new ListenerForSubject(subject, new DataGenerator<TOPIC>());
    }

}
