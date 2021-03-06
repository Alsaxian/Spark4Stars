import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import schema.PetaSkySchema

object Spark4StarsDraft2 {

  val compte = "p1715490" // Changer en utilisant votre login

  def countByObjectId(inputFilename: String, sc: SparkContext): RDD[(String, Long)] = {
    sc.textFile(inputFilename)
      .map(_.split(",").map(_.trim)) // transformer chaque ligne en un tableau de String
      .filter(l => l(PetaSkySchema.s_objectId) != "NULL")
      .map(l => (l(PetaSkySchema.s_objectId), 1L)) // on associe 1 à chaque clé
      .reduceByKey((count1, count2) => count1 + count2)
  }

  def main(args: Array[String]): Unit = {
    if (args.length > 0) {
      val conf = new SparkConf().setAppName("Spark4StarsDraft2-" + compte)
      val sc = new SparkContext(conf)
      val result = countByObjectId(args(0), sc)
      if (args.length > 1) {
        result
          .map(r => "%s,%s".format(r._1, r._2))
          .saveAsTextFile(args(1))
      } else {
        result.collect().foreach(println)
      }
    } else {
      println("Usage: spark-submit --class Spark4StarsDraft2 /home/" + compte + "/SparkTPApp-correction-assembly-1.0.jar " +
        "hdfs:///user/" + compte + "/repertoire-donnees " +
        "[hdfs:///user/" + compte + "/repertoire-resultat]")
    }
  }
}
