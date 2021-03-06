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

package voldemort.store.socket.clientrequest;

import voldemort.client.protocol.RequestFormat;
import voldemort.common.nio.ByteBufferBackedOutputStream;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.versioning.Version;
import voldemort.server.RequestRoutingType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DeleteClientRequest extends AbstractStoreClientRequest<Boolean> {

    private final ByteArray key;

    private final Version version;

    public DeleteClientRequest(String storeName,
                               RequestFormat requestFormat,
                               RequestRoutingType requestRoutingType,
                               ByteArray key,
                               Version version) {
        super(storeName, requestFormat, requestRoutingType);
        this.key = key;
        this.version = version;
    }

    public boolean isCompleteResponse(ByteBuffer buffer) {
        return requestFormat.isCompleteDeleteResponse(buffer);
    }

    @Override
    protected void formatRequestInternal(ByteBufferBackedOutputStream outputStream)
            throws IOException {
        requestFormat.writeDeleteRequest(new DataOutputStream(outputStream),
                                         storeName,
                                         key,
                                         version,
                                         requestRoutingType);
    }

    @Override
    protected Boolean parseResponseInternal(DataInputStream inputStream) throws IOException {
        return requestFormat.readDeleteResponse(inputStream);
    }

}
