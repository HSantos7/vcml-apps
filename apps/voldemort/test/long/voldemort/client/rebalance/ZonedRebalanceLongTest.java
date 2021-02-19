/*
 * Copyright 2013 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package voldemort.client.rebalance;


/**
 * Run a version of RebalanceTests with a lot more keys.
 * 
 */
public class ZonedRebalanceLongTest extends AbstractZonedRebalanceTest {

    private final int NUM_KEYS = 10100;

    public ZonedRebalanceLongTest() {
        super();
    }

    @Override
    protected int getNumKeys() {
        return NUM_KEYS;
    }

}
