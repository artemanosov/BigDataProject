package schema

import java.sql.Timestamp

case class ExtendedTweet(
  created_at: String,
  id: Long,
  id_str: String,
  text: String,
  source: String,
  truncated: Boolean,
  in_reply_to_status_id: Long,
  in_reply_to_status_id_str: String,
  in_reply_to_user_id: Long,
  in_reply_to_user_id_str: String,
  in_reply_to_str_name: String,
  user: User,
  coordinates: String,
  place: String,
  quoted_status_id: Long,
  quoted_status_id_str: String,
  is_quote_status: Boolean,
  quoted_status: String,
  retweeted_status: String,
  quote_count: Integer,
  reply_count: Integer,
  retweet_count: Integer,
  favorite_count: Integer,
  entities: String,
  extended_entities: String,
  favorited: Boolean,
  retweeted: Boolean,
  possibly_sensitive: Boolean,
  filter_level: String,
  lang: String,
  matching_rules: String
)

case class User(
  id: Long,
  id_str: String,
  name: String,
  screen_name: String,
  location: String,
  derived: String,
  url: String,
  description: String,
  protected_field: Boolean,
  verified: Boolean,
  followers_count: Integer,
  friends_count: Integer,
  listed_count: Integer,
  favourites_count: Integer,
  statuses_count: Integer,
  created_at: String,
  profile_banner_url: String,
  profile_image_url_https: String,
  default_profile: Boolean,
  default_profile_image: Boolean,
  withheld_in_countries: String,
  withheld_scope: String
)

case class Tweet(
  id: Long,
  user: String,
  text: String,
  created_at: Timestamp,
  processed_at: Timestamp
)