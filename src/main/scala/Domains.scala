import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import scala.io.Source

//case class domain(name: String, count: BigInt)
//case class Row(name: String, count: BigInt)

/*---------------------------------------------------------------------------------------*/
object Domains {

    val locale = new java.util.Locale("us", "US")
    val formatter = java.text.NumberFormat.getIntegerInstance(locale)

    /*-------------------------------------------------------------------------------------*/
    def main(args: Array[String]) {

        val table_name = if (args(0).length == 0) "vdomain" else args(0)
        val file_name  = if (args(1).length == 0) { 
            Console.err.println("Need filename argument")
            sys.exit(1) 
        } else  args(1) 

        println("+----------------------------------------------------------")
        println("| " + table_name + " | " + file_name)
        println("+----------------------------------------------------------")

        get_largest_visited_domains(table_name, file_name)
    }


    /**
      * https://docs.databricks.com/spark/latest/data-sources/cassandra.html
      *
      * @param table_name
      * @param size
      */
    def get_largest_visited_domains(table_name: String, file_name: String): Unit = {


        val spark = SparkSession
            .builder()
            .appName("TopDomains")
            .config("spark.cassandra.connection.host", "10.0.0.1")
            .getOrCreate()


        val df1 = spark.read
            .cassandraFormat("vdomain", "cloud1", "Cassandra Cluster")
            .load().cache()

        val diseases = Source.fromFile(file_name).getLines.toArray 

        for(disease <- diseases) {

            /*--- Read from Cassandra ---*/
            df1.createOrReplaceTempView("table1")
            val SQL = "SELECT * FROM table1 WHERE domain like '%"+disease+"%'"
            println(SQL)
            val df2 = spark.sql(SQL)
            df2.show(10, false)


            /*--- Write to Cassandra ---*/
            df2.write
                .format("org.apache.spark.sql.cassandra")
                .mode("append") //.mode("overwrite")
                .options(Map("table" -> "health", "keyspace" -> "cloud2"))
                .save()
        }

        spark.stop()

    }

}
/*-------------------------------------------------------------------------------------------*/
//  val df2 = df1.orderBy(col("total").desc)
//  df2.show(50, false)
//  println("Total vdomain = " + df2.count())
/*-------------------------------------------------------------------------------------------*/
