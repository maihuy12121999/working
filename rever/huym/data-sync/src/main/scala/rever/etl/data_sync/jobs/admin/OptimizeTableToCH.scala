package rever.etl.data_sync.jobs.admin

import org.apache.commons.io.FileUtils
import rever.etl.data_sync.core.Runner
import rever.etl.data_sync.core.clickhouse.{ClickhouseSink, SystemPartDAO}
import rever.etl.rsparkflow.api.configuration.Config

import java.util
import scala.jdk.CollectionConverters.asScalaBufferConverter

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class OptimizeTableToCH(config: Config) extends Runner {

  private val optimizedDatabases = config
    .getList(
      "optimize_database_watchlist",
      ",",
      util.Arrays.asList("analytics", "finance_analytics")
    )
    .asScala
    .distinct

  private val maxSizeInBytes = config.getLong("max_size_in_bytes", 100022914L)

  override def run(): Long = {

    var totalOptimizePartition = 0

    val systemPartDAO = SystemPartDAO(
      ds = ClickhouseSink.client(config),
      tableName = "system.parts"
    )

    val partitions = systemPartDAO.selectPartitionRequiredToMerge(optimizedDatabases, maxSizeInBytes)

    partitions.foreach { case (database, table, partitionId) =>
      systemPartDAO.optimizePartition(database, table, partitionId)

      println(s"$database - $table - $partitionId")
      totalOptimizePartition += 1
    }

    println(s"""
         |Size threshold: ${FileUtils.byteCountToDisplaySize(maxSizeInBytes)}
         |Total partitions: ${partitions.size}
         |Total optimized partitions: ${totalOptimizePartition}/${partitions.size} partitions.
         |""".stripMargin)

    totalOptimizePartition
  }

}
