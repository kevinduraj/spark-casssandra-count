import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._


case class domain(name: String, count: BigInt)

case class Row(name: String, count: BigInt)

/*---------------------------------------------------------------------------------------*/
object Domains {

        val diseases=Array(
                "abscess"
                ,"allergy"
                ,"anemia"
                ,"angiomatosis"
                ,"appendicitis"
                ,"aspergillosis"
                ,"autism"
                ,"bacterial"
                ,"biochemical"
                ,"biology"
                ,"bladder"
                ,"bone"
                ,"brain"    
                ,"bronchitis"
                ,"cancer"
                ,"cancer"  
                ,"carcinoma"
                ,"cardio"   
                ,"cervical"
                ,"chlamydial"
                ,"choriomeningitis"
                ,"chromozone"
                ,"chronic"
                ,"collagen"
                ,"congenita"
                ,"contagious"
                ,"cortical"
                ,"deafness"
                ,"dental"
                ,"diabetic"
                ,"disease"  
                ,"disorder"
                ,"dissection"
                ,"drug"
                ,"emboli"
                ,"eustachian"
                ,"fatty"
                ,"fever"
                ,"fungal"   
                ,"gastric"
                ,"gastrointestinal"
                ,"genetic"
                ,"genetics"
                ,"gerson"  
                ,"goitre"
                ,"gout"
                ,"granulomatosis"
                ,"headache" 
                ,"health"  
                ,"heart"    
                ,"heatstroke"
                ,"hematologic"
                ,"hemoglobinopathies"
                ,"hemorrhagic"
                ,"hepatitis"
                ,"hormonal"
                ,"hutchinson"
                ,"hyperactivity"
                ,"hypertension"
                ,"intestinal"
                ,"intravascular"
                ,"kidney"
                ,"liver"
                ,"lung"
                ,"lyme"
                ,"lymphocytic"
                ,"lymphomatoid"
                ,"meningococcal"
                ,"mental"   
                ,"metabolic"
                ,"myofascial"
                ,"nasal"
                ,"necrosis"
                ,"nematodes"
                ,"osteoporosis"
                ,"overdose"
                ,"pain"
                ,"paraneoplastic"
                ,"parkinsons"
                ,"phenomena"
                ,"prednisone"
                ,"prostatic"
                ,"pulmonary"
                ,"renal"
                ,"respiratory"
                ,"schizoaffective"
                ,"schizophrenia"
                ,"sickness"
                ,"skin"
                ,"sleeping"
                ,"spinal"
                ,"surgery"
                ,"symptoms"
                ,"syndrome"
                ,"tissue"
                ,"treatment"
                ,"tuberculosis"
                ,"tumor"
                ,"vascular"
                ,"virus"
                ,"visual"
                ,"vitamin"
                ,"vulgaris"
        )

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
            .load().cache()


        for(disease <- diseases) {

            //--- Read from Cassandra
            df1.createOrReplaceTempView("table1")
            val SQL = "SELECT * FROM table1 WHERE domain like '%"+disease+"%'"
            println(SQL)
            val df2 = spark.sql(SQL)
            df2.show(10, false)


            //--- Write to Cassandra
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
