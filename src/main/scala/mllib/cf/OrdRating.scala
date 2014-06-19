package mllib.cf

import org.apache.spark.mllib.recommendation.Rating

/**
 * Created by phoenix on 5/8/14.
 */
class OrdRating extends Ordering[Rating] {
  def compare(one: Rating, another: Rating) = one.rating compare another.rating
}
