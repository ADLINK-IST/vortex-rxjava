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

import org.omg.dds.sub.Sample;
import rx.functions.Func1;

class SampleGenerator<TOPIC> implements Func1<Sample<TOPIC>, Sample<TOPIC>> {
    @Override
    public Sample<TOPIC> call(Sample<TOPIC> o) {
        return o;
    }
}
