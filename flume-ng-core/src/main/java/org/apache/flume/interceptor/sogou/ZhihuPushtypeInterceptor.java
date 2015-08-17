/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor.sogou;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.flume.interceptor.sogou.ZhihuPushtypeInterceptor.Constants.*;

public class ZhihuPushtypeInterceptor implements Interceptor {

  private final boolean preserveExisting;

  /**
   * Only {@link org.apache.flume.interceptor.sogou.ZhihuPushtypeInterceptor.Builder} can build me
   */
  private ZhihuPushtypeInterceptor(boolean preserveExisting) {
    this.preserveExisting = preserveExisting;
  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    if (preserveExisting && headers.containsKey(PUSH_TYPE)) {
      // we must preserve the existing timestamp
    } else {
      String basename = headers.get("basename");
      String pushType = ZhihuUtils.getPushTypeFromFileName(basename);
      headers.put(PUSH_TYPE, pushType);
    }
    return event;
  }

  /**
   * Delegates to {@link #intercept(org.apache.flume.Event)} in a loop.
   *
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instances of the ZhihuPushtypeInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private boolean preserveExisting = PRESERVE_DFLT;
    private Pattern regex;
    private Charset charset = Charsets.UTF_8;

    @Override
    public Interceptor build() {
      return new ZhihuPushtypeInterceptor(preserveExisting);
    }

    @Override
    public void configure(Context context) {
      preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
    }

  }

  public static class Constants {
    public static final String PUSH_TYPE = "pushtype";
    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DFLT = false;
  }
}
