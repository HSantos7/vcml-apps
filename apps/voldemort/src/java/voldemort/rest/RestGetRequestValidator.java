package voldemort.rest;

import org.apache.log4j.Logger;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import voldemort.store.CompositeGetAllVoldemortRequest;
import voldemort.store.CompositeGetVersionVoldemortRequest;
import voldemort.store.CompositeGetVoldemortRequest;
import voldemort.store.CompositeVoldemortRequest;
import voldemort.consistency.utils.ByteArray;

/**
 * This class is used to parse incoming get and get all requests. Parses and
 * validates the REST Request and constructs a CompositeVoldemortRequestObject.
 * Also Handles exceptions specific to get and get all operations.
 */
public class RestGetRequestValidator extends RestRequestValidator {

    protected boolean isGetVersionRequest = false;
    private final Logger logger = Logger.getLogger(RestGetRequestValidator.class);

    public RestGetRequestValidator(HttpRequest request, MessageEvent messageEvent) {
        super(request, messageEvent);
    }

    /**
     * Validations specific to GET and GET ALL
     */
    @Override
    public boolean parseAndValidateRequest() {
        if(!super.parseAndValidateRequest()) {
            return false;
        }
        isGetVersionRequest = hasGetVersionRequestHeader();
        if(isGetVersionRequest && this.parsedKeys.size() > 1) {
            RestErrorHandler.writeErrorResponse(messageEvent,
                                                HttpResponseStatus.BAD_REQUEST,
                                                "Get version request cannot have multiple keys");
            return false;
        }
        return true;
    }

    public boolean hasGetVersionRequestHeader() {
        boolean result = false;
        String headerValue = this.request.getHeader(RestMessageHeaders.X_VOLD_GET_VERSION);
        if(headerValue != null) {
            result = true;
        }
        return result;
    }

    @Override
    public CompositeVoldemortRequest<ByteArray, byte[]> constructCompositeVoldemortRequestObject() {
        if(parseAndValidateRequest()) {
            if(this.isGetVersionRequest) {
                if(logger.isDebugEnabled()) {
                    debugLog("GET_VERSION", System.currentTimeMillis());
                }
                this.requestObject = new CompositeGetVersionVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                                this.parsedTimeoutInMs,
                                                                                                this.parsedRequestOriginTimeInMs,
                                                                                                this.parsedRoutingType);

            } else if(this.parsedKeys.size() > 1) {
                if(logger.isDebugEnabled()) {
                    debugLog("GET_ALL", System.currentTimeMillis());
                }
                this.requestObject = new CompositeGetAllVoldemortRequest<ByteArray, byte[]>(this.parsedKeys,
                                                                                            this.parsedTimeoutInMs,
                                                                                            this.parsedRequestOriginTimeInMs,
                                                                                            this.parsedRoutingType);
            } else {
                if(logger.isDebugEnabled()) {
                    debugLog("GET", System.currentTimeMillis());
                }
                this.requestObject = new CompositeGetVoldemortRequest<ByteArray, byte[]>(this.parsedKeys.get(0),
                                                                                         this.parsedTimeoutInMs,
                                                                                         this.parsedRequestOriginTimeInMs,
                                                                                         this.parsedRoutingType);
            }
            return this.requestObject;
        }
        // Return null if request is not valid
        return null;
    }

}
