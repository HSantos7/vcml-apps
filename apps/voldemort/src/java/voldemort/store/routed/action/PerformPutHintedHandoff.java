/*
 * Copyright 2010 LinkedIn, Inc
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

package voldemort.store.routed.action;

import voldemort.consistency.cluster.Node;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;
import voldemort.store.routed.Pipeline;
import voldemort.store.routed.PutPipelineData;
import voldemort.store.slop.HintedHandoff;
import voldemort.store.slop.Slop;
import voldemort.utils.Time;

import java.util.Date;

public class PerformPutHintedHandoff extends AbstractHintedHandoffAction<Void, PutPipelineData> {

    private final Versioned<byte[]> versioned;

    private final Time time;

    private final byte[] transforms;

    public PerformPutHintedHandoff(PutPipelineData pipelineData,
                                   Pipeline.Event completeEvent,
                                   ByteArray key,
                                   Versioned<byte[]> versioned,
                                   byte[] transforms,
                                   HintedHandoff hintedHandoff,
                                   Time time) {
        super(pipelineData, completeEvent, key, hintedHandoff);
        this.versioned = versioned;
        this.time = time;
        this.transforms = transforms;
    }

    @Override
    public void execute(Pipeline pipeline) {
        Versioned<byte[]> versionedCopy = pipelineData.getVersionedCopy();
        for(Node slopFinalDestinationNode: pipelineData.getSynchronizer().getDelegatedSlopDestinations()) {
            int failedNodeId = slopFinalDestinationNode.getId();
            if(versionedCopy == null) {
                Version clock = versioned.getVersion();
                versionedCopy = new Versioned<byte[]>(versioned.getValue(),
                                                      clock.incremented(failedNodeId,
                                                                        time.getMilliseconds()));
            }

            Version version = versionedCopy.getVersion();
            if(logger.isTraceEnabled())
                logger.trace("Performing parallel hinted handoff for node " + slopFinalDestinationNode
                             + ", store " + pipelineData.getStoreName() + " key " + key
                             + ", version " + version);

            Slop slop = new Slop(pipelineData.getStoreName(),
                                 Slop.Operation.PUT,
                                 key,
                                 versionedCopy.getValue(),
                                 transforms,
                                 failedNodeId,
                                 new Date());
            hintedHandoff.sendHintParallel(slopFinalDestinationNode, version, slop);
        }
        pipeline.addEvent(completeEvent);
    }
}
