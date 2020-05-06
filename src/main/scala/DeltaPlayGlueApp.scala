import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
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

    // Find the latest registration for each first_name+last_name based on the 'registration_dttm' timestamp
    // Note: For nested structs, max on struct is computed as max on first struct field, if equal fall back to second fields, and so on.
    val latestChangeForEachKey = userdf
      .selectExpr("first_name", "last_name", "struct(registration_dttm, id, email, gender, ip_address, cc, country, salary, birthdate, title, comments) as otherCols" )
      .groupBy("first_name", "last_name")
      .agg(max("otherCols").as("latest"))
      .selectExpr("first_name", "last_name", "latest.*")

    logger.info(s"Total deduplicated updates to process: ${latestChangeForEachKey.count()}")

    DeltaTable.forPath(spark, DELTA_LOCATION)
      .as("users")
      .merge(
        latestChangeForEachKey.as("updates"),
        "users.last_name = updates.last_name and users.first_name = updates.first_name")
      .whenMatched
      .updateAll()
      .whenNotMatched
      .insertAll()
      .execute()

    logger.info(s"Compact table to 1 parquet file")

    spark.read
      .format("delta")
      .load(DELTA_LOCATION)
      .repartition(1)  // TODO: calculate that number depending on table size
      .write
      .option("dataChange", "false")
      .format("delta")
      .mode("overwrite")
      .save(DELTA_LOCATION)

    logger.info(s"Generating manifest")

    val deltaTable = DeltaTable.forPath(spark, DELTA_LOCATION)
    deltaTable.generate("symlink_format_manifest")

    deltaTable.history.show

    logger.info(s"Commiting job...")
    Job.commit()
    logger.info(s"Job finished.")
  }
}
