package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.execution.{SparkOptimizer, SparkPlanner}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.{SQLConf, SessionState, SharedState}
import org.apache.spark.sql.optimizer.CarbonOptimizer

import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties

class CarbonSessionState(
    val sparkSession: SparkSession,
    val storePath: String)
  extends SessionState(sparkSession) {
  self =>

  override lazy val sqlParser: ParserInterface = new CarbonSpark2Parser(conf)
  private lazy val sharedState: HiveSharedState = {
    sparkSession.sharedState.asInstanceOf[HiveSharedState]
  }
  override lazy val conf: SQLConf = new CarbonSQLConf
  override lazy val catalog = {
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
    new CarbonSessionCatalog(
      sharedState.externalCatalog,
      metadataHive,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      newHadoopConf(),
      storePath)
  }



  /**
   * A Hive client used for interacting with the metastore.
   */
  lazy val metadataHive: HiveClient = sharedState.metadataHive.newSession()

  /**
   * An analyzer that uses the Hive metastore.
   */
  override lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        catalog.ParquetConversions ::
        catalog.OrcConversions ::
        catalog.CreateTables ::
        PreprocessTableInsertion(conf) ::
        DataSourceAnalysis(conf) ::
        (if (conf.runSQLonFile) new ResolveDataSource(sparkSession) :: Nil else Nil)

      override val extendedCheckRules = Seq(PreWriteCheck(conf, catalog))
    }
  }


  override lazy val optimizer: Optimizer = new CarbonOptimizer(new SparkOptimizer(catalog, conf, experimentalMethods), catalog, conf)

  /**
   * Planner that takes into account Hive-specific strategies.
   */
  override def planner: SparkPlanner = {
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)
      with HiveStrategies {
      override val sparkSession: SparkSession = self.sparkSession

      override def strategies: Seq[Strategy] = {
        val carbonStrategy = new CarbonStrategies(self)
        experimentalMethods.extraStrategies ++ Seq(carbonStrategy.CarbonTableScan, carbonStrategy.DDLStrategies)++ Seq(
          FileSourceStrategy,
          DataSourceStrategy,
          DDLStrategy,
          SpecialLimits,
          InMemoryScans,
          HiveTableScans,
          DataSinks,
          Scripts,
          Aggregation,
          JoinSelection,
          BasicOperators
        )
      }
    }
  }


  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  override def addJar(path: String): Unit = {
    metadataHive.addJar(path)
    super.addJar(path)
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the parquet SerDe
   * are automatically converted to use the Spark SQL parquet table scan, instead of the Hive
   * SerDe.
   */
  def convertMetastoreParquet: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET)
  }

  /**
   * When true, also tries to merge possibly different but compatible Parquet schemas in different
   * Parquet data files.
   *
   * This configuration is only effective when "spark.sql.hive.convertMetastoreParquet" is true.
   */
  def convertMetastoreParquetWithSchemaMerging: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_PARQUET_WITH_SCHEMA_MERGING)
  }

  /**
   * When true, enables an experimental feature where metastore tables that use the Orc SerDe
   * are automatically converted to use the Spark SQL ORC table scan, instead of the Hive
   * SerDe.
   */
  def convertMetastoreOrc: Boolean = {
    conf.getConf(HiveUtils.CONVERT_METASTORE_ORC)
  }

  /**
   * When true, Hive Thrift server will execute SQL queries asynchronously using a thread pool."
   */
  def hiveThriftServerAsync: Boolean = {
    conf.getConf(HiveUtils.HIVE_THRIFT_SERVER_ASYNC)
  }

  // TODO: why do we get this from SparkConf but not SQLConf?
  def hiveThriftServerSingleSession: Boolean = {
    sparkSession.sparkContext.conf.getBoolean(
      "spark.sql.hive.thriftServer.singleSession", defaultValue = false)
  }


}
