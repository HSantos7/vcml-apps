/*
 * Copyright 2008-2013 LinkedIn, Inc
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

package voldemort.restclient;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.PRECONDITION_FAILED;
import static org.jboss.netty.handler.codec.http.HttpResponseStatus.REQUEST_TIMEOUT;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.mail.MessagingException;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.util.ByteArrayDataSource;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import voldemort.VoldemortException;
import voldemort.common.VoldemortOpCode;
import voldemort.rest.RestMessageHeaders;
import voldemort.rest.RestUtils;
import voldemort.rest.VectorClockWrapper;
import voldemort.store.AbstractStore;
import voldemort.store.InsufficientOperationalNodesException;
import voldemort.consistency.utils.ByteArray;
import voldemort.consistency.versioning.ObsoleteVersionException;
import voldemort.consistency.versioning.VectorClock;
import voldemort.consistency.versioning.Version;
import voldemort.consistency.versioning.Versioned;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.common.util.None;
import com.linkedin.data.ByteString;
import com.linkedin.r2.message.rest.RestException;
import com.linkedin.r2.message.rest.RestRequest;
import com.linkedin.r2.message.rest.RestRequestBuilder;
import com.linkedin.r2.message.rest.RestResponse;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.r2.transport.common.bridge.client.TransportClient;
import com.linkedin.r2.transport.common.bridge.client.TransportClientAdapter;

/**
 * A class that implements the Store interface for interacting with the RESTful
 * Coordinator. It leverages the R2 library for doing this.
 *
 */
public class R2Store extends AbstractStore<ByteArray, byte[], byte[]> {

    private static final String GET = "GET";
    private static final String POST = "POST";
    private static final String DELETE = "DELETE";
    public static final String CONTENT_TYPE = "Content-Type";
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CUSTOM_RESOLVING_STRATEGY = "custom";
    public static final String DEFAULT_RESOLVING_STRATEGY = "timestamp";
    public static final String SCHEMATA_STORE_NAME = "schemata";
    private static final String MULTIPART_CONTENT_TYPE = "multipart/binary";
    private static final String FETCH_SCHEMA_TIMEOUT_MS = "50000";
    private static final int INVALID_ZONE_ID = -1;
    private final Logger logger = Logger.getLogger(R2Store.class);

    private Client client = null;
    private String restBootstrapURL;
    private ObjectMapper mapper;
    private RESTClientConfig config;
    private String routingTypeCode = null;
    private int zoneId;
    private AtomicBoolean isActive;

    public R2Store(String storeName,
                   String restBootstrapURL,
                   final TransportClient transportClient,
                   final RESTClientConfig config) {
        this(storeName, restBootstrapURL, null, transportClient, null, config, INVALID_ZONE_ID);
    }

    public R2Store(String storeName,
                   String restBootstrapURL,
                   String routingCodeStr,
                   final TransportClient transportClient,
                   final RESTClientConfig config,
                   int zoneId) {
        this(storeName, restBootstrapURL, routingCodeStr, transportClient, null, config, zoneId);
    }

    public R2Store(String storeName,
                   String restBootstrapURL,
                   Client d2Client,
                   RESTClientConfig config) {
        this(storeName, restBootstrapURL, null, null, d2Client, config, INVALID_ZONE_ID);
    }

    public R2Store(String storeName,
                   String restBootstrapURL,
                   String routingCodeStr,
                   final TransportClient transportClient,
                   Client d2Client,
                   RESTClientConfig config,
                   int zoneId) {
        super(storeName);
        if(transportClient == null) {
            this.client = d2Client;
        } else {
            this.client = new TransportClientAdapter(transportClient);
        }
        this.config = config;
        this.restBootstrapURL = restBootstrapURL;
        this.mapper = new ObjectMapper();
        this.routingTypeCode = routingCodeStr;
        this.zoneId = zoneId;
        this.isActive = new AtomicBoolean(true);
    }

    @Override
    public void close() throws VoldemortException {
        final FutureCallback<None> clientShutdownCallback = new FutureCallback<None>();
        if(!this.isActive.get()) {
            return;
        }

        if(client != null) {
            client.shutdown(clientShutdownCallback);
            try {
                clientShutdownCallback.get();
                this.isActive.compareAndSet(true, false);
            } catch(InterruptedException e) {
                logger.error("Interrupted while shutting down the HttpClientFactory: "
                                     + e.getMessage(),
                             e);
            } catch(ExecutionException e) {
                logger.error("Execution exception occurred while shutting down the HttpClientFactory: "
                                     + e.getMessage(),
                             e);
            }
        }
    }

    @Override
    public boolean delete(ByteArray key, Version version) throws VoldemortException {
        try {
            // Create the REST request with this byte array
            String base64Key = RestUtils.encodeVoldemortKey(key.get());
            RestRequestBuilder rb = new RestRequestBuilder(new URI(this.restBootstrapURL + "/"
                                                                   + getName() + "/" + base64Key));
            // Create a HTTP POST request
            rb.setMethod(DELETE);
            rb.setHeader(CONTENT_LENGTH, "0");
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.DELETE_OP_CODE));
            rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                         String.valueOf(System.currentTimeMillis()));
            if(this.routingTypeCode != null) {
                rb.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE, this.routingTypeCode);
            }
            if(this.zoneId != INVALID_ZONE_ID) {
                rb.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID, String.valueOf(this.zoneId));
            }
            // Serialize the Vector clock
            VectorClock vc = (VectorClock) version;

            // If the given Vector clock is empty, we'll let the receiver of
            // this request fetch the existing vector clock and increment
            // before
            // doing the put.
            if(vc != null && vc.getEntries().size() != 0) {
                String serializedVC = null;
                if(!vc.getEntries().isEmpty()) {
                    serializedVC = RestUtils.getSerializedVectorClock(vc);
                }

                if(serializedVC != null && serializedVC.length() > 0) {
                    rb.setHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, serializedVC);
                }
            }

            RestRequest request = rb.build();
            Future<RestResponse> f = client.restRequest(request);

            // This will block
            RestResponse response = f.get();
            final ByteString entity = response.getEntity();
            if(entity == null) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Empty response !");
                }
            }

        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }
                if(exception.getResponse().getStatus() == NOT_FOUND.getCode()) {
                    return false;
                }
            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage());
            }
            throw new VoldemortException("Operation Interrupted: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        }
        return true;
    }

    private RestResponse fetchGetResponse(RestRequestBuilder requestBuilder, String timeoutStr)
            throws InterruptedException, ExecutionException {
        requestBuilder.setMethod(GET);
        requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
        requestBuilder.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                                 String.valueOf(System.currentTimeMillis()));
        if(this.routingTypeCode != null) {
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE,
                                     this.routingTypeCode);
        }
        if(this.zoneId != INVALID_ZONE_ID) {
            requestBuilder.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID, String.valueOf(this.zoneId));
        }
        RestRequest request = requestBuilder.build();
        Future<RestResponse> f = client.restRequest(request);
        // This will block
        return f.get();
    }

    @Override
    public List<Versioned<byte[]>> get(ByteArray key, byte[] transforms) throws VoldemortException {
        List<Versioned<byte[]>> resultList = new ArrayList<Versioned<byte[]>>();
        String base64Key = RestUtils.encodeVoldemortKey(key.get());
        RestRequestBuilder rb = null;
        try {
            rb = new RestRequestBuilder(new URI(this.restBootstrapURL + "/" + getName() + "/"
                                                + base64Key));
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.GET_OP_CODE));
            rb.setHeader("Accept", MULTIPART_CONTENT_TYPE);

            RestResponse response = fetchGetResponse(rb, timeoutStr);
            final ByteString entity = response.getEntity();
            if(entity != null) {
                resultList = parseGetResponse(entity);
            } else {
                if(logger.isDebugEnabled()) {
                    logger.debug("Did not get any response!");
                }
            }
        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }

            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        }

        return resultList;
    }

    private List<Versioned<byte[]>> parseGetResponse(ByteString entity) {
        List<Versioned<byte[]>> results = new ArrayList<Versioned<byte[]>>();

        try {
            // Build the multipart object
            byte[] bytes = new byte[entity.length()];
            entity.copyBytes(bytes, 0);

            ByteArrayDataSource ds = new ByteArrayDataSource(bytes, "multipart/mixed");
            MimeMultipart mp = new MimeMultipart(ds);
            for(int i = 0; i < mp.getCount(); i++) {
                MimeBodyPart part = (MimeBodyPart) mp.getBodyPart(i);
                String serializedVC = part.getHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK)[0];
                int contentLength = Integer.parseInt(part.getHeader(RestMessageHeaders.CONTENT_LENGTH)[0]);

                if(logger.isDebugEnabled()) {
                    logger.debug("Received VC : " + serializedVC);
                }
                VectorClockWrapper vcWrapper = mapper.readValue(serializedVC,
                                                                VectorClockWrapper.class);

                InputStream input = part.getInputStream();
                byte[] bodyPartBytes = new byte[contentLength];
                input.read(bodyPartBytes);

                VectorClock clock = new VectorClock(vcWrapper.getVersions(),
                                                    vcWrapper.getTimestamp());
                results.add(new Versioned<byte[]>(bodyPartBytes, clock));

            }

        } catch(MessagingException e) {
            throw new VoldemortException("Messaging exception while trying to parse GET response "
                                         + e.getMessage(), e);
        } catch(JsonParseException e) {
            throw new VoldemortException("JSON parsing exception while trying to parse GET response "
                                                 + e.getMessage(),
                                         e);
        } catch(JsonMappingException e) {
            throw new VoldemortException("JSON mapping exception while trying to parse GET response "
                                                 + e.getMessage(),
                                         e);
        } catch(IOException e) {
            throw new VoldemortException("IO exception while trying to parse GET response "
                                         + e.getMessage(), e);
        }
        return results;

    }

    public String getSerializerInfoXml() throws VoldemortException {
        RestRequestBuilder rb = null;
        try {
            String base64Key = new String(Base64.encodeBase64(getName().getBytes("UTF-8")));
            rb = new RestRequestBuilder(new URI(this.restBootstrapURL + "/" + SCHEMATA_STORE_NAME
                                                + "/" + base64Key));
            rb.setHeader("Accept", "binary");
            rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                         String.valueOf(System.currentTimeMillis()));
            if(this.routingTypeCode != null) {
                rb.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE, this.routingTypeCode);
            }
            if(this.zoneId != INVALID_ZONE_ID) {
                rb.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID, String.valueOf(this.zoneId));
            }

            RestResponse response = fetchGetResponse(rb, FETCH_SCHEMA_TIMEOUT_MS);
            return response.getEntity().asString("UTF-8");
        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }
            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        } catch(UnsupportedEncodingException e) {
            throw new VoldemortException("Unsupported Encoding exception while encoding the key"
                                         + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public void put(ByteArray key, Versioned<byte[]> value, byte[] transform)
            throws VoldemortException {
        RestResponse response = null;

        try {
            byte[] payload = value.getValue();

            // Create the REST request with this byte array
            String base64Key = RestUtils.encodeVoldemortKey(key.get());
            RestRequestBuilder rb = new RestRequestBuilder(new URI(this.restBootstrapURL + "/"
                                                                   + getName() + "/" + base64Key));

            // Create a HTTP POST request
            rb.setMethod(POST);
            rb.setEntity(payload);
            rb.setHeader(CONTENT_TYPE, "binary");
            rb.setHeader(CONTENT_LENGTH, "" + payload.length);
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.PUT_OP_CODE));
            rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
            rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                         String.valueOf(System.currentTimeMillis()));
            if(this.routingTypeCode != null) {
                rb.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE, this.routingTypeCode);
            }
            if(this.zoneId != INVALID_ZONE_ID) {
                rb.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID, String.valueOf(this.zoneId));
            }

            // Serialize the Vector clock
            VectorClock vc = (VectorClock) value.getVersion();

            // If the given Vector clock is empty, we'll let the receiver of
            // this request fetch the existing vector clock and increment before
            // doing the put.
            if(vc != null) {
                String serializedVC = null;
                if(!vc.getEntries().isEmpty()) {
                    serializedVC = RestUtils.getSerializedVectorClock(vc);
                }

                if(serializedVC != null && serializedVC.length() > 0) {
                    rb.setHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK, serializedVC);
                }
            }

            RestRequest request = rb.build();
            Future<RestResponse> f = client.restRequest(request);

            // This will block
            response = f.get();

            String serializedUpdatedVC = response.getHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK);
            if(serializedUpdatedVC == null || serializedUpdatedVC.length() == 0) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Received empty vector clock in the response");
                }
            } else {
                VectorClock updatedVC = RestUtils.deserializeVectorClock(serializedUpdatedVC);
                VectorClock originalVC = (VectorClock) value.getVersion();
                originalVC.copyFromVectorClock(updatedVC);
            }

            final ByteString entity = response.getEntity();
            if(entity == null) {
                if(logger.isDebugEnabled()) {
                    logger.debug("Empty response !");
                }
            }
        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }

                int httpErrorStatus = exception.getResponse().getStatus();
                if(httpErrorStatus == BAD_REQUEST.getCode()) {
                    throw new VoldemortException("Bad request: " + e.getMessage(), e);
                } else if(httpErrorStatus == PRECONDITION_FAILED.getCode()) {
                    throw new ObsoleteVersionException(e.getMessage());
                } else if(httpErrorStatus == REQUEST_TIMEOUT.getCode()
                          || httpErrorStatus == INTERNAL_SERVER_ERROR.getCode()) {
                    throw new InsufficientOperationalNodesException(e.getMessage());
                }
            }
            throw new VoldemortException("Unknown HTTP request execution exception: "
                                         + e.getMessage(), e);

        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage());
            }
            throw new VoldemortException("Unknown Voldemort exception: " + e.getMessage());
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage());
        }
    }

    @Override
    public Map<ByteArray, List<Versioned<byte[]>>> getAll(Iterable<ByteArray> keys,
                                                          Map<ByteArray, byte[]> transforms)
            throws VoldemortException {

        Map<ByteArray, List<Versioned<byte[]>>> resultMap = new HashMap<ByteArray, List<Versioned<byte[]>>>();
        int numberOfKeys = 0;

        try {
            Iterator<ByteArray> it = keys.iterator();
            StringBuilder keyArgs = null;

            while(it.hasNext()) {
                ByteArray key = it.next();
                String base64Key = RestUtils.encodeVoldemortKey(key.get());
                if(keyArgs == null) {
                    keyArgs = new StringBuilder();
                    keyArgs.append(base64Key);
                } else {
                    keyArgs.append("," + base64Key);
                }
                numberOfKeys++;
            }

            // Rerouting getall() requests with single key to get(). This is a
            // temporary fix to handle the NPE when getAll requests are made
            // with single key.
            // TODO a common way to handle getAll with any number of keys
            if(numberOfKeys == 1) {
                List<Versioned<byte[]>> resultList = new ArrayList<Versioned<byte[]>>();
                it = keys.iterator();
                ByteArray key = it.next();
                byte[] singleKeyTransforms = null;
                if(transforms != null) {
                    singleKeyTransforms = transforms.get(key);
                }
                resultList = this.get(key, singleKeyTransforms);
                resultMap.put(key, resultList);
            } else {

                RestRequestBuilder rb = new RestRequestBuilder(new URI(this.restBootstrapURL + "/"
                                                                       + getName() + "/"
                                                                       + keyArgs.toString()));

                rb.setMethod(GET);
                rb.setHeader("Accept", MULTIPART_CONTENT_TYPE);
                String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                             .getOperationTimeout(VoldemortOpCode.GET_ALL_OP_CODE));
                rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_TIMEOUT_MS, timeoutStr);
                rb.setHeader(RestMessageHeaders.X_VOLD_REQUEST_ORIGIN_TIME_MS,
                             String.valueOf(System.currentTimeMillis()));
                if(this.routingTypeCode != null) {
                    rb.setHeader(RestMessageHeaders.X_VOLD_ROUTING_TYPE_CODE, this.routingTypeCode);
                }
                if(this.zoneId != INVALID_ZONE_ID) {
                    rb.setHeader(RestMessageHeaders.X_VOLD_ZONE_ID, String.valueOf(this.zoneId));
                }

                RestRequest request = rb.build();
                Future<RestResponse> f = client.restRequest(request);

                // This will block
                RestResponse response = f.get();

                // Parse the response
                final ByteString entity = response.getEntity();

                String contentType = response.getHeader(CONTENT_TYPE);
                if(entity != null) {
                    if(contentType.equalsIgnoreCase(MULTIPART_CONTENT_TYPE)) {

                        resultMap = parseGetAllResults(entity);
                    } else {
                        if(logger.isDebugEnabled()) {
                            logger.debug("Did not receive a multipart response");
                        }
                    }

                } else {
                    if(logger.isDebugEnabled()) {
                        logger.debug("Did not get any response!");
                    }
                }
            }
        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }
            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        }

        return resultMap;
    }

    private Map<ByteArray, List<Versioned<byte[]>>> parseGetAllResults(ByteString entity) {
        Map<ByteArray, List<Versioned<byte[]>>> results = new HashMap<ByteArray, List<Versioned<byte[]>>>();

        try {
            // Build the multipart object
            byte[] bytes = new byte[entity.length()];
            entity.copyBytes(bytes, 0);

            // Get the outer multipart object
            ByteArrayDataSource ds = new ByteArrayDataSource(bytes, "multipart/mixed");
            MimeMultipart mp = new MimeMultipart(ds);
            for(int i = 0; i < mp.getCount(); i++) {

                // Get an individual part. This contains all the versioned
                // values for a particular key referenced by content-location
                MimeBodyPart part = (MimeBodyPart) mp.getBodyPart(i);

                // Get the key
                String contentLocation = part.getHeader("Content-Location")[0];
                String base64Key = contentLocation.split("/")[2];
                ByteArray key = new ByteArray(RestUtils.decodeVoldemortKey(base64Key));

                if(logger.isDebugEnabled()) {
                    logger.debug("Content-Location : " + contentLocation);
                    logger.debug("Base 64 key : " + base64Key);
                }

                // Create an array list for holding all the (versioned values)
                List<Versioned<byte[]>> valueResultList = new ArrayList<Versioned<byte[]>>();

                /*
                 * Get the nested Multi-part object. This contains one part for each unique versioned value.
                 *
                 * GetContent method can corrupt the embedded data, for example 0x8c be converted to 0xc2, 0x8c,
                 * hence use getInputStream.
                 *
                 * This thread tracks this question
                 * http://stackoverflow.com/questions/23023583/mimebodypart-getcontent-corrupts-binary-data
                 *
                 * getInputStream() : Return a decoded input stream for this Message's "content.
                 *
                 * getRawInputStream() : Return an InputStream to the raw data with any Content-Transfer-Encoding
                 * intact. This method is useful if the "Content-Transfer-Encoding" header is incorrect or corrupt,
                 * which would prevent the getInputStream method from returning the correct data. In such a case
                 * the application may use this method and attempt to decode the raw data itself.
                 *
                 */

                ByteArrayDataSource nestedDS = new ByteArrayDataSource(part.getInputStream(),
                                                                       "multipart/mixed");
                MimeMultipart valueParts = new MimeMultipart(nestedDS);

                for(int valueId = 0; valueId < valueParts.getCount(); valueId++) {

                    MimeBodyPart valuePart = (MimeBodyPart) valueParts.getBodyPart(valueId);
                    String serializedVC = valuePart.getHeader(RestMessageHeaders.X_VOLD_VECTOR_CLOCK)[0];
                    int contentLength = Integer.parseInt(valuePart.getHeader(RestMessageHeaders.CONTENT_LENGTH)[0]);

                    if(logger.isDebugEnabled()) {
                        logger.debug("Received serialized Vector Clock : " + serializedVC);
                    }

                    VectorClockWrapper vcWrapper = mapper.readValue(serializedVC,
                                                                    VectorClockWrapper.class);

                    // get the value bytes
                    InputStream input = valuePart.getInputStream();
                    byte[] bodyPartBytes = new byte[contentLength];
                    input.read(bodyPartBytes);

                    VectorClock clock = new VectorClock(vcWrapper.getVersions(),
                                                        vcWrapper.getTimestamp());
                    valueResultList.add(new Versioned<byte[]>(bodyPartBytes, clock));

                }
                results.put(key, valueResultList);
            }

        } catch(MessagingException e) {
            throw new VoldemortException("Messaging exception while trying to parse GET response "
                                         + e.getMessage(), e);
        } catch(JsonParseException e) {
            throw new VoldemortException("JSON parsing exception while trying to parse GET response "
                                                 + e.getMessage(),
                                         e);
        } catch(JsonMappingException e) {
            throw new VoldemortException("JSON mapping exception while trying to parse GET response "
                                                 + e.getMessage(),
                                         e);
        } catch(IOException e) {
            throw new VoldemortException("IO exception while trying to parse GET response "
                                         + e.getMessage(), e);
        }
        return results;

    }

    @Override
    public List<Version> getVersions(ByteArray key) {
        List<Version> resultList = new ArrayList<Version>();
        String base64Key = RestUtils.encodeVoldemortKey(key.get());
        RestRequestBuilder rb = null;
        try {
            rb = new RestRequestBuilder(new URI(this.restBootstrapURL + "/" + getName() + "/"
                                                + base64Key));
            String timeoutStr = Long.toString(this.config.getTimeoutConfig()
                                                         .getOperationTimeout(VoldemortOpCode.GET_VERSION_OP_CODE));

            rb.setHeader(RestMessageHeaders.X_VOLD_GET_VERSION, "true");

            RestResponse response = fetchGetResponse(rb, timeoutStr);
            final ByteString entity = response.getEntity();
            if(entity != null) {
                resultList = parseGetVersionResponse(entity);
            } else {
                if(logger.isDebugEnabled()) {
                    logger.debug("Did not get any response!");
                }
            }
        } catch(ExecutionException e) {
            if(e.getCause() instanceof RestException) {
                RestException exception = (RestException) e.getCause();
                if(logger.isDebugEnabled()) {
                    logger.debug("REST EXCEPTION STATUS : " + exception.getResponse().getStatus());
                }

            } else {
                throw new VoldemortException("Unknown HTTP request execution exception: "
                                             + e.getMessage(), e);
            }
        } catch(InterruptedException e) {
            if(logger.isDebugEnabled()) {
                logger.debug("Operation interrupted : " + e.getMessage(), e);
            }
            throw new VoldemortException("Operation interrupted exception: " + e.getMessage(), e);
        } catch(URISyntaxException e) {
            throw new VoldemortException("Illegal HTTP URL" + e.getMessage(), e);
        }

        return resultList;
    }

    private List<Version> parseGetVersionResponse(ByteString entity) {
        byte[] bytes = new byte[entity.length()];
        entity.copyBytes(bytes, 0);
        String vectorClockListStr = new String(bytes);
        List<Version> vectorClockList = RestUtils.deserializeVectorClocks(vectorClockListStr);
        return vectorClockList;
    }

}
