package life.genny.fyodor.utils;

import org.jboss.logging.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * A utility class for date and time related operations.
 * 
 * @author Jasper Robison
 */
public class TimeUtils {

	static final Logger log = Logger.getLogger(TimeUtils.class);

	/** 
	 * Format a LocalTime object to a string
	 *
	 * @param time the time to be formatted
	 * @param format the format to use
	 * @return String
	 */
	public static String formatTime(LocalTime time, String format) {

		if (time != null && format != null) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

			return time.format(formatter);
		}

		return null;
	}

	/** 
	 * Format a LocalDate object to a string
	 *
	 * @param date the date to be formatted
	 * @param format the format to use
	 * @return String
	 */
	public static String formatDate(LocalDate date, String format) {

		if (date != null && format != null) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

			return date.format(formatter);
		}

		return null;
	}

	/** 
	 * Format a LocalDateTime object to a string
	 *
	 * @param dateTime the dateTime to format
	 * @param format the format to use
	 * @return String
	 */
	public static String formatDateTime(LocalDateTime dateTime, String format) {

		if (dateTime != null && format != null) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

			return dateTime.format(formatter);
		}

		return null;
	}

	/** 
	 * Format a ZonedDateTime object to a string
	 *
	 * @param dateTime the zoned dateTime to be formatted
	 * @param format the format to use
	 * @return String
	 */
	public static String formatZonedDateTime(ZonedDateTime dateTime, String format) {

		if (dateTime != null && format != null) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);

			return dateTime.format(formatter);
		}

		return null;
	}

}
