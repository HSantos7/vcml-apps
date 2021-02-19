package voldemort.rest;

import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static org.jboss.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.log4j.Logger;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;

public class RestErrorHandler {

    protected final Logger logger = Logger.getLogger(getClass());

    /**
     * Exceptions specific to each operation is handled in the corresponding
     * subclass. At this point we don't know the reason behind this exception.
     * 
     * @param exception
     */
    protected void handleExceptions(MessageEvent messageEvent, Exception exception) {
        logger.error("Unknown exception. Internal Server Error.", exception);
        writeErrorResponse(messageEvent,
                           HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           "Internal Server Error");
    }

    /**
     * Writes all error responses to the client.
     * 
     * TODO REST-Server 1. collect error stats
     * 
     * @param messageEvent - for retrieving the channel details
     * @param status - error code
     * @param message - error message
     */
    public static void writeErrorResponse(MessageEvent messageEvent,
                                          HttpResponseStatus status,
                                          String message) {

        // Create the Response object
        HttpResponse response = new DefaultHttpResponse(HTTP_1_1, status);
        response.setHeader(CONTENT_TYPE, "text/plain; charset=UTF-8");
        response.setContent(ChannelBuffers.copiedBuffer("Failure: " + status.toString() + ". "
                                                        + message + "\r\n", CharsetUtil.UTF_8));
        response.setHeader(CONTENT_LENGTH, response.getContent().readableBytes());

        // Write the response to the Netty Channel
        messageEvent.getChannel().write(response);
    }
}
