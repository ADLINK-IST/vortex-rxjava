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

import com.prismtech.vortex.rx.RxVortex;
import org.omg.dds.core.*;
import org.omg.dds.core.status.DataAvailableStatus;
import org.omg.dds.core.status.Status;
import org.omg.dds.sub.DataReader;
import org.omg.dds.sub.Sample;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.schedulers.Schedulers;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class OnSubscribeFromReaderWaitset<T> implements Observable.OnSubscribe<T> {

    private final DataReader<T> dataReader;
    private final Object mutex;
    private final Scheduler.Worker worker;
    private final Duration timeout;
    private WaitSet ws = null;
    private StatusCondition<DataReader<T>> statusCondition;
    private final Subject<T, T> subject;


    public OnSubscribeFromReaderWaitset(DataReader<T> dr, Duration timeout) {
        this.dataReader = dr;
        this.mutex = new Object();
        worker = Schedulers.io().createWorker();
        this.timeout = timeout;
        subject = PublishSubject.create();
    }

    private void initializeWaitset() {
        statusCondition = dataReader.getStatusCondition();
        List<Class<? extends Status>> statuses = new ArrayList<Class<? extends Status>>();
        statuses.add(DataAvailableStatus.class);
        statusCondition.setEnabledStatuses(statuses);
        ServiceEnvironment environment = ServiceEnvironment.createInstance(RxVortex.class.getClassLoader());
        ws = environment.getSPI().newWaitSet();
        ws.attachCondition(statusCondition);

        final GuardCondition terminated = environment.getSPI().newGuardCondition();
        ws.attachCondition(terminated);
    }

    private void receiveData() {
        int count = 0;
//        Collection<Condition> conditions;
        while(true) {
            try {
//                conditions = ws.waitForConditions(timeout);
                ws.waitForConditions(timeout);
//                for(Condition activeCondition : conditions) {
//                    if(statusCondition.equals(activeCondition)){
                        final Sample.Iterator<T> datum = dataReader.take();
                        while (datum.hasNext()) {
                            Sample<T> sample = datum.next();
                            final T data = sample.getData();
                            if(data != null) {
                                subject.onNext(data);
                            }
                        }
//                    }
//                }
            } catch (TimeoutException e) {
                // TODO : Log the timeout
                continue;
            }
        }
    }



    @Override
    public void call(Subscriber<? super T> subscriber) {
        subject.subscribe(subscriber);
        synchronized (mutex) {
            if(ws == null) {
                initializeWaitset();
                worker.schedule(
                        new Action0() {
                            @Override
                            public void call() {
                                receiveData();
                            }
                        });
            }
        }
    }
}
