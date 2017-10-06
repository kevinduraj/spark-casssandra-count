import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.io.Source

//case class domain(name: String, count: BigInt)
//case class Row(name: String, count: BigInt)

/*---------------------------------------------------------------------------------------*/
object ExportLinks {

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

        export_health_domains(table_name, file_name)
        cassandra_table_count("links")

    }


    /**
      * https://docs.databricks.com/spark/latest/data-sources/cassandra.html
      * @param table_name
      * @param table 
      */
    def export_health_domains(table_name: String, file_name: String): Unit = {


        val spark = SparkSession
            .builder()
            .appName("ExportLinks")
            .config("spark.cassandra.connection.host", "10.0.0.1")
            .getOrCreate()


        val df1 = spark.read
            .cassandraFormat(table_name, "cloud1", "Cassandra Cluster")
            .load().cache()
        df1.createOrReplaceTempView("table1")

        val diseases = Source.fromFile(file_name).getLines.toArray 
        for(disease <- diseases) {

            /*--- Read from Cassandra ---*/
            val SQL = "SELECT * FROM table1 WHERE url like '%"+disease+"%'"
            println(SQL)
            val df2 = spark.sql(SQL)
            df2.show(10, false)

            /*--- Write to Cassandra ---*/
            df2.write
                .format("org.apache.spark.sql.cassandra")
                .mode("append") //.mode("overwrite")
                .options(Map("table" -> table_name, "keyspace" -> "cloud2"))
                .save()
        }
        
        spark.stop()

    }

    def cassandra_table_count(table: String): Unit = {

        val conf = new SparkConf(true)
            .setAppName("Table Count")
            .set("spark.cassandra.connection.host", "10.0.0.1")
        val sc = new SparkContext(conf)
        val rdd = sc.cassandraTable("cloud2", table)

        println("+----------------------------------------------------------")
        println("  Total cloud2." + table + " = " + rdd.count)
        println("+----------------------------------------------------------")

    }

}
/*-------------------------------------------------------------------------------------------*/
//  val df2 = df1.orderBy(col("total").desc)
//  df2.show(50, false)
//  println("Total vdomain = " + df2.count())
/*-------------------------------------------------------------------------------------------*/
