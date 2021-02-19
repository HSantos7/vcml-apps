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
package voldemort.store.routed;

import voldemort.client.ClientConfig;
import voldemort.client.TimeoutConfig;
import voldemort.client.ZoneAffinity;
import voldemort.consistency.cluster.Cluster;
import voldemort.consistency.cluster.Zone;
import voldemort.server.VoldemortConfig;

public class RoutedStoreConfig {

    private boolean repairReads = true;
    private TimeoutConfig timeoutConfig = new TimeoutConfig();
    private boolean isJmxEnabled = false;
    private String identifierString = "";
    private int clientZoneId = Zone.UNSET_ZONE_ID;
    private ZoneAffinity zoneAffinity = new ZoneAffinity();

    public RoutedStoreConfig() {}

    public RoutedStoreConfig(ClientConfig clientConfig) {
        this.isJmxEnabled = clientConfig.isJmxEnabled();
        this.clientZoneId = clientConfig.getClientZoneId();
        this.timeoutConfig = clientConfig.getTimeoutConfig();
        this.zoneAffinity = clientConfig.getZoneAffinity();
    }

    public RoutedStoreConfig(VoldemortConfig voldemortConfig, Cluster cluster) {
        this.isJmxEnabled = voldemortConfig.isJmxEnabled();
        this.timeoutConfig = voldemortConfig.getTimeoutConfig();
        // clientZoneId being server zone id for server side routing
        this.clientZoneId = cluster.getNodeById(voldemortConfig.getNodeId()).getZoneId();
    }

    public boolean getRepairReads() {
        return repairReads;
    }

    public RoutedStoreConfig setRepairReads(boolean repairReads) {
        this.repairReads = repairReads;
        return this;
    }

    public TimeoutConfig getTimeoutConfig() {
        return timeoutConfig;
    }

    public RoutedStoreConfig setTimeoutConfig(TimeoutConfig timeoutConfig) {
        this.timeoutConfig = timeoutConfig;
        return this;
    }

    public int getClientZoneId() {
        return clientZoneId;
    }

    public RoutedStoreConfig setClientZoneId(int clientZoneId) {
        this.clientZoneId = clientZoneId;
        return this;
    }

    public boolean isJmxEnabled() {
        return isJmxEnabled;
    }

    public RoutedStoreConfig setJmxEnabled(boolean isJmxEnabled) {
        this.isJmxEnabled = isJmxEnabled;
        return this;
    }

    public String getIdentifierString() {
        return identifierString;
    }

    public RoutedStoreConfig setIdentifierString(String identifierString) {
        this.identifierString = identifierString;
        return this;
    }

    public ZoneAffinity getZoneAffinity() {
        return zoneAffinity;
    }

    public RoutedStoreConfig setZoneAffinity(ZoneAffinity zoneAffinity) {
        this.zoneAffinity = zoneAffinity;
        return this;
    }
}
