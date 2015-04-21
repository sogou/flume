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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flume.interceptor.sogou.TimestampInterceptor.Constants.*;

/**
 * Simple Interceptor class that sets the current system timestamp on all events
 * that are intercepted.
 * By convention, this timestamp header is named "timestamp" and its format
 * is a "stringified" long timestamp in milliseconds since the UNIX epoch.
 */
public class TimestampInterceptor implements Interceptor {

	private final boolean preserveExisting;
	private final Pattern regex;
	private final Charset charset;

	/**
	 * Only {@link TimestampInterceptor.Builder} can build me
	 */
	private TimestampInterceptor(boolean preserveExisting, Pattern regex, Charset charset) {
		this.regex = regex;
		this.preserveExisting = preserveExisting;
		this.charset = charset;
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
		if (preserveExisting && headers.containsKey(TIMESTAMP)) {
			// we must preserve the existing timestamp
		} else {
			String origBody = new String(event.getBody(), charset);
			Matcher m = regex.matcher(origBody);
			if (m.find()) {
				String timestamp = m.group(1);
				String message = m.group(2);
				event.setBody(message.getBytes(charset));
				headers.put(TIMESTAMP, timestamp + "000");
			} else {
				long now = System.currentTimeMillis();
				headers.put(TIMESTAMP, Long.toString(now));
			}
		}
		return event;
	}

	/**
	 * Delegates to {@link #intercept(Event)} in a loop.
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
	 * Builder which builds new instances of the TimestampInterceptor.
	 */
	public static class Builder implements Interceptor.Builder {

		private boolean preserveExisting = PRESERVE_DFLT;
		private Pattern regex;
		private Charset charset = Charsets.UTF_8;

		@Override
		public Interceptor build() {
			return new TimestampInterceptor(preserveExisting, regex, charset);
		}

		@Override
		public void configure(Context context) {
			String regexString = context.getString(REGEX, DEFAULT_REGEX);
			regex = Pattern.compile(regexString);

			preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);

			if (context.containsKey(CHARSET_KEY)) {
				// May throw IllegalArgumentException for unsupported charsets.
				charset = Charset.forName(context.getString(CHARSET_KEY));
			}
		}

	}

	public static class Constants {
		public static final String TIMESTAMP = "timestamp";
		public static final String PRESERVE = "preserveExisting";
		public static final boolean PRESERVE_DFLT = false;

		public static final String REGEX = "regex";
		public static final String DEFAULT_REGEX = "^(\\d{10}):(.*)$";

		public static final String CHARSET_KEY = "charset";
	}
}
