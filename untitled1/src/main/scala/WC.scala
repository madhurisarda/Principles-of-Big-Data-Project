import org.apache.spark.{SparkConf, SparkContext}
object WC
{
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir","C:\\Users\\madhuri's PC\\Downloads\\winutils-master\\hadoop-2.8.3");
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc=new SparkContext(sparkConf)
    val inputf=sc.wholeTextFiles("D:\\Masters\\PB_Priciples_ofBig_Data\\Project\\Extracted_Output_File.txt",minPartitions=2)
    inputf.map(abs=>{
      abs._1
      abs._2
    })
    val wc=inputf.flatMap(abs=>{abs._2.split(" ")}).map(word=>(word,1))
    val output=wc.reduceByKey(_+_)
    output.saveAsTextFile("wordcountspark1")
    val o=output.collect()
    var s:String="Words:Count \n"
    o.foreach{case(word,count)=>{
      s+=word+" : "+count+"\n"
    }
    }
  }
}
