import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

// Manual imports
import com.amazonaws.services.glue.log.GlueLogger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.functions._

object GlueApp {
  def main(sysArgs: Array[String]) {

    val sc: SparkContext = new SparkContext(new SparkConf().set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"))

    val glueContext: GlueContext = new GlueContext(sc)

    val spark: SparkSession = glueContext.getSparkSession
    import spark.implicits._

    val logger = new GlueLogger

    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "RAW_DATA_LOCATION", "DELTA_LOCATION").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    val DATA_LOCATION = args("RAW_DATA_LOCATION")
    val DELTA_LOCATION= args("DELTA_LOCATION")

    logger.info(s"Reading ${DATA_LOCATION}")

    val userdf = spark.read.format("parquet").load(s"${DATA_LOCATION}")

    userdf.write.format("delta").mode("overwrite").save(DELTA_LOCATION)


    logger.info(s"Generating manifest")

    val deltaTable = DeltaTable.forPath(spark, DELTA_LOCATION)
    deltaTable.generate("symlink_format_manifest")

    deltaTable.history.show

    logger.info(s"Commiting job...")
    Job.commit()
    logger.info(s"Job finished.")
  }
}
