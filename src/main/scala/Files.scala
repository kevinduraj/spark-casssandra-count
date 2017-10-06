import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import scala.io.Source

/*---------------------------------------------------------------------------------------*/
object Files {

    val locale = new java.util.Locale("us", "US")
    val formatter = java.text.NumberFormat.getIntegerInstance(locale)

    /*-------------------------------------------------------------------------------------*/
    def main(args: Array[String]) {

        val file_name = if (args(0).length == 0) "vdomain" else args(0)

        println("+---------------------------------------------")
        println("| Reading: " + file_name   ) 
        println("+---------------------------------------------")

        val diseases = Source.fromFile(file_name).getLines.toArray 
        
        for(disease <- diseases) {
            println(disease)
        }
    }
}
/*-------------------------------------------------------------------------------------------*/
//  val df2 = df1.orderBy(col("total").desc)
//  df2.show(50, false)
//  println("Total vdomain = " + df2.count())
/*-------------------------------------------------------------------------------------------*/
