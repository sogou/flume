package org.apache.flume.instrumentation.sogou;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Tao Li on 4/29/15.
 */
public class TimeUtils {
	private static final String FIVE_MIN_TIME_FOTMAT = "yyyyMMddHHmm";
	private static SimpleDateFormat fiveMinSDF = new SimpleDateFormat(FIVE_MIN_TIME_FOTMAT);

	public static String convertTimestampStrToFiveMinStr(String timestampStr) {
		long timestamp = (long) Math.floor(Long.parseLong(timestampStr) / 300000) * 300000;
		return fiveMinSDF.format(new Date(timestamp));
	}
}
