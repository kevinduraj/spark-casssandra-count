/**
 * Written by: Kevin Duraj 
 * Spark 2.0 with Cassandra 3.7 
 */

import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._
import org.apache.spark.sql._

object TotalLinksInDallas {

    val locale = new java.util.Locale("us", "US")
     val formatter = java.text.NumberFormat.getIntegerInstance(locale)

    def main(args: Array[String]) {

        val conf = new SparkConf(true)
                        .setAppName("TotalLinksInDallas")
                        .set("spark.cassandra.connection.host", "10.0.0.1")

        //val sc = new SparkContext("spark://69.13.39.46:7077", "cloud1", conf)
        val sc = new SparkContext(conf)

        val rdd = sc.cassandraTable("cloud1", "links")

        println("-----------------------------------------")
        println("Total Links: " + rdd.count)
        println(rdd.first)
        //println(rdd.map(_.getInt("url")).sum)
        println("-----------------------------------------")

        sc.stop();

    }

}
