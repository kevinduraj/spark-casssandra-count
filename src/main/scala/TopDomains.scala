import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._


case class domain(name: String, count: BigInt)

case class Row(name: String, count: BigInt)

/*---------------------------------------------------------------------------------------*/
object TopDomains {

    val locale = new java.util.Locale("us", "US")
    val formatter = java.text.NumberFormat.getIntegerInstance(locale)

    /*-------------------------------------------------------------------------------------*/
    def main(args: Array[String]) {

        val table_name = if (args(0).length == 0) "vdomain" else args(0)
        val size = if (args(1).length == 0) 10000 else args(1).toInt

        println("*********************************************")
        println(table_name + " " + size)
        println("*********************************************")

        get_largest_visited_domains(table_name, size)
    }


    /**
      * https://docs.databricks.com/spark/latest/data-sources/cassandra.html
      *
      * @param table_name
      * @param size
      */
    def get_largest_visited_domains(table_name: String, size: Int): Unit = {

        val spark = SparkSession
            .builder()
            .appName("TopDomains")
            .config("spark.cassandra.connection.host", "10.0.0.1")
            .getOrCreate()

        val df1 = spark.read
            .cassandraFormat("vdomain", "cloud1", "Cassandra Cluster")
            .load()

        val df2 = df1.orderBy(col("total").desc)
        df2.show(50, false)
        println("Total vdomain = " + df2.count())

        val df3 = spark.sql("SELECT * FROM vdomain WHERE total < 10")
        df3.show(50, false)

        // Write to Cassandra
        //      employee1.write
        //          .format("org.apache.spark.sql.cassandra")
        //          .mode("overwrite")
        //          .options(Map( "table" -> "employee_new", "keyspace" -> "test_keyspace"))
        //          .save()


        //val result = table1.select("domain").groupBy("domain")
        //val df2 = df.select("domain", "total").filter("total > " + size).orderBy("total")
        //df2.collect().foreach { row => println(row.get(0)  + " " + row.get(1) ) }

        // df2.filter("count > 1000").collect().foreach(println)
        // csc.setKeyspace("engine")
        // df2.registerTempTable("table1")
        // csc.sql("SELECT * FROM table1").show()

        //println("--------------------------------------------------------------")
        //df2.collect().foreach { row => println(row.get(0)  + " " + row.get(1) ) }
        //println("--------------------------------------------------------------")

        spark.stop()

    }

    /*---------------------------------------------------------------------------------------*/

}
