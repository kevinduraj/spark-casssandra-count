import com.datastax.spark.connector._
import org.apache.spark.{SparkContext, SparkConf}

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

    println(table_name + " " + size)
    get_largest_visited_domains(table_name, size)
  }

  /*-------------------------------------------------------------------------------------*/  
  def get_largest_visited_domains(table_name: String, size: Int): Unit = {
        
        val conf = new SparkConf(true)
                        .setAppName("TopDomains")
                        .set("spark.cassandra.connection.host", "10.0.0.1")
        //val sc = new SparkContext("spark://69.13.39.46:7077", "cloud1", conf)
        val spark = new SparkContext(conf)
        
        val df = spark.cassandraTable("cloud1", table_name)
        val total = df.cassandraCount()
        println("Total " +  table_name + " = " + total)

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
