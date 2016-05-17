package org.apache.flume.source.http;

import com.google.common.io.CharStreams;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Tao Li on 5/17/16.
 */
public class POSTBodyHandler implements HTTPSourceHandler {
  private static final Logger LOG = LoggerFactory.getLogger(POSTBodyHandler.class);

  public List<Event> getEvents(HttpServletRequest request) throws Exception {
    if (!"POST".equalsIgnoreCase(request.getMethod())) {
      LOG.error("Unsupported method {}. Only supports method POST", request.getMethod());
      throw new HTTPBadRequestException("Only supports POST method");
    }

    String charset = request.getCharacterEncoding();
    if (charset == null) {
      LOG.debug("Charset is null, default charset of UTF-8 will be used.");
      charset = "UTF-8";
    } else if (!(charset.equalsIgnoreCase("utf-8")
        || charset.equalsIgnoreCase("utf-16")
        || charset.equalsIgnoreCase("utf-32"))) {
      LOG.error("Unsupported character set in request {}. "
          + "Only supports UTF-8, "
          + "UTF-16 and UTF-32 only.", charset);
      throw new UnsupportedCharsetException("Only supports UTF-8, "
          + "UTF-16 and UTF-32 only.");
    }

    List<Event> eventList = new ArrayList<Event>();
    String body = CharStreams.toString(request.getReader());
    Event event = EventBuilder.withBody(body.getBytes(charset), new HashMap<String, String>());
    eventList.add(event);

    return eventList;

  }

  public void configure(Context context) {

  }
}
