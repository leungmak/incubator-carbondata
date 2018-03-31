package org.apache.carbondata.store;

import java.io.IOException;
import java.util.Map;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.sdk.file.CarbonReader;
import org.apache.carbondata.spark.rdd.CarbonScanRDD;

import org.apache.spark.CarbonInputMetrics;
import org.apache.spark.sql.CarbonSession;
import org.apache.spark.sql.SparkSession;

public class CarbonStoreOnSpark implements CarbonStore {
  public CarbonStoreOnSpark(String engine, Map<String, String> params) {
    super(engine, params);
  }

  private CarbonSession session;

  public CarbonStoreOnSpark() {
    session = SparkSession
        .builder()
        .master(masterUrl)
        .appName(appName)
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.driver.host", "localhost")
        .config("spark.sql.crossJoin.enabled", "true")
        .getOrCreateCarbonSession(storeLocation, metastoredb);
  }

  @Override
  public Row[] query(String path, String[] projectColumns, Expression filter)
      throws IOException {
    TableInfo schema = CarbonReader.readSchemaFile(CarbonTablePath.getSchemaFilePath(path));
    CarbonTable table = CarbonTable.buildFromTableInfo(schema);
    CarbonScanRDD rdd = new CarbonScanRDD(
        session,
        new CarbonProjection(projectColumns),
        filter,
        table.getAbsoluteTableIdentifier(),
        table.getTableInfo().serialize(),
        table.getTableInfo(),
        new CarbonInputMetrics(),
        null);

//    table.getTableInfo().getFactTable().getListOfColumns();

    return rdd.map(
        v1 -> new MutableRow(),
        scala.reflect.ClassTag$.MODULE$.apply(CarbonRow.class)
    ).collect();
  }

  @Override public Row[] sql(String sqlString) throws IOException {
    return new MutableRow[0];
  }

}
