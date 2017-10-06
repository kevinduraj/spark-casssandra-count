import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._


case class domain(name: String, count: BigInt)
case class Row(name:String, count:BigInt)

/*---------------------------------------------------------------------------------------*/  
object TopDomains {

  val locale = new java.util.Locale("us", "US")
  val formatter = java.text.NumberFormat.getIntegerInstance(locale)
  
  /*-------------------------------------------------------------------------------------*/  
  def main(args: Array[String]) {

      val table_name = if(args(0).length == 0) "vdomain" else args(0)
      val size = if(args(1).length == 0) 10000 else args(1).toInt

      println("*********************************************")
      println(table_name + " " + size)
      println("*********************************************")

      get_largest_visited_domains(table_name, size)
  }

  /*-------------------------------------------------------------------------------------*/  
  def get_largest_visited_domains(table_name: String, size: Int): Unit = {
        
//        val conf = new SparkConf(true)
//                        .setAppName("TopDomains")
//                        .set("spark.cassandra.connection.host", "10.0.0.1")
//        //val sc = new SparkContext("spark://69.13.39.46:7077", "cloud1", conf)
//        val spark = new SparkContext(conf)
//
//        val table1 = spark.cassandraTable("cloud1", table_name)
//        val total = table1.cassandraCount()
//        println("Total " +  table_name + " = " + total)
//
//
//        val table2 = table1.spanBy(row => (row.getString("domain")))
//        table2.groupByKey.count
//        table2.take(100).foreach(println)
//        println(table2.getClass)
      /*-------------------------------------------------------------------------------------*/
      import org.apache.spark.sql.SparkSession

      val spark = SparkSession
          .builder()
          .appName("TopDomains")
          .config("spark.cassandra.connection.host", "10.0.0.1")
          .getOrCreate()

      val df1 = spark.read
          .cassandraFormat("vdomain", "cloud1", "Cassandra Cluster")
          .load()

            df1.show(100, false)

      //val result1 = table2.count()
        //println(result1)

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
