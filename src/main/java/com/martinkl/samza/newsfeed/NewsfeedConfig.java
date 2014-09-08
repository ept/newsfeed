package com.martinkl.samza.newsfeed;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.TimeZone;

import org.apache.samza.system.SystemStream;

public class NewsfeedConfig {
  public static final SystemStream FOLLOWS_STREAM = new SystemStream("kafka", "newsfeed-follows");
  public static final SystemStream MESSAGES_STREAM = new SystemStream("kafka", "newsfeed-messages");
  public static final SystemStream DELIVERIES_STREAM = new SystemStream("kafka", "newsfeed-deliveries");

  public static final int NUM_USERS = 1000;
  public static final int AVERAGE_FOLLOWERS_PER_USER = 50;
  public static final long NUM_FOLLOW_EVENTS = NUM_USERS * AVERAGE_FOLLOWERS_PER_USER;
  public static final int MESSAGES_PER_WINDOW = 1;

  public static final String DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";

  private static final Random random = new Random();

  public static String randomUser() {
    return "u" + random.nextInt(NUM_USERS);
  }

  public static String currentDateTime() {
    SimpleDateFormat dateFormat = new SimpleDateFormat(DATETIME_FORMAT);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dateFormat.format(new Date());
  }
}
