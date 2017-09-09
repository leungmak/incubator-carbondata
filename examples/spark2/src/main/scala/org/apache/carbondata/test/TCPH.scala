/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.test

import java.io.File

import org.apache.spark.sql.SparkSession

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonV3DataFormatConstants}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.test.TPCHDataSizeTest.blockletSize

object TCPH {

  def main(args: Array[String]): Unit = {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    val storeLocation = s"$rootPath/examples/spark2/target/store"
    val warehouse = s"$rootPath/examples/spark2/target/warehouse"
    val metastoredb = s"$rootPath/examples/spark2/target"

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy-MM-dd HH:mm:ss")
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy-MM-dd")
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE_LOADING, "true")
      .addProperty(CarbonV3DataFormatConstants.BLOCKLET_SIZE_IN_MB, blockletSize)

    import org.apache.spark.sql.CarbonSession._
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("CarbonSessionExample")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.driver.host", "localhost")
      .getOrCreateCarbonSession(storeLocation, metastoredb)

    spark.sparkContext.setLogLevel("WARN")

//    createTable(spark)
//    loadData(spark)
    runQuery(spark)
  }

  private def createTable(spark: SparkSession) = {
    spark.sql("""drop database if exists tpchcarbon_default cascade""")
    spark.sql("""create database tpchcarbon_default""")
    spark.sql("""use tpchcarbon_default""")

    spark
      .sql(
        """create table if not exists SUPPLIER(S_COMMENT string,S_SUPPKEY string,S_NAME string,
          |S_ADDRESS string, S_NATIONKEY string, S_PHONE string, S_ACCTBAL double)
          |STORED BY 'org.apache.carbondata.format'
          |TBLPROPERTIES ('DICTIONARY_EXCLUDE'='S_COMMENT, S_SUPPKEY,
          |S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE','table_blocksize'='300','SORT_COLUMNS'='')"""
          .stripMargin)
    spark
      .sql(
        """create table if not exists PARTSUPP (  PS_PARTKEY int,  PS_SUPPKEY  string,
          |PS_AVAILQTY  int,  PS_SUPPLYCOST  double,  PS_COMMENT  string)
          |STORED BY 'org.apache.carbondata.format'
          |TBLPROPERTIES ('DICTIONARY_EXCLUDE'='PS_SUPPKEY,PS_COMMENT',
          |'table_blocksize'='300', 'no_inverted_index'='PS_SUPPKEY, PS_COMMENT',
          |'SORT_COLUMNS'='')""".stripMargin)
    spark
      .sql(
        """create table if not exists CUSTOMER(  C_MKTSEGMENT string,  C_NATIONKEY string,
          |C_CUSTKEY string,  C_NAME string,  C_ADDRESS string,  C_PHONE string,  C_ACCTBAL
          |double,  C_COMMENT string)
          |STORED BY 'org.apache.carbondata.format'
          |TBLPROPERTIES('DICTIONARY_INCLUDE'='C_MKTSEGMENT,C_NATIONKEY','DICTIONARY_EXCLUDE'='C_CUSTKEY,
          |C_NAME,C_ADDRESS,C_PHONE,C_COMMENT', 'table_blocksize'='300',
          |'no_inverted_index'='C_CUSTKEY,C_NAME,C_ADDRESS,C_PHONE,C_COMMENT',
          |'SORT_COLUMNS'='C_MKTSEGMENT')""".stripMargin)
    spark
      .sql(
        """create table if not exists NATION (  N_NAME string,  N_NATIONKEY string,  N_REGIONKEY
          |string,  N_COMMENT  string)
          |STORED BY 'org.apache.carbondata.format'
          | TBLPROPERTIES
          |('DICTIONARY_EXCLUDE'='N_COMMENT', 'table_blocksize'='300',
          |'no_inverted_index'='N_COMMENT','SORT_COLUMNS'='N_NAME')""".stripMargin)
    spark
      .sql(
        """create table if not exists REGION(  R_NAME string,  R_REGIONKEY string,  R_COMMENT
          |string)
          |STORED BY 'org.apache.carbondata.format'
          |TBLPROPERTIES
          |('DICTIONARY_INCLUDE'='R_NAME,R_REGIONKEY','DICTIONARY_EXCLUDE'='R_COMMENT',
          |'table_blocksize'='300','no_inverted_index'='R_COMMENT','SORT_COLUMNS'='R_NAME')"""
          .stripMargin)
    spark
      .sql(
        """create table if not exists PART(  P_BRAND string,  P_SIZE int,  P_CONTAINER string,
          |P_TYPE string,  P_PARTKEY INT ,  P_NAME string,  P_MFGR string,  P_RETAILPRICE double,
          |  P_COMMENT string)
          |  STORED BY 'org.apache.carbondata.format'
          |  TBLPROPERTIES
          |  ('DICTIONARY_INCLUDE'='P_BRAND,P_SIZE,P_CONTAINER,P_MFGR',
          |  'DICTIONARY_EXCLUDE'='P_NAME, P_COMMENT', 'table_blocksize'='300',
          |  'no_inverted_index'='P_NAME,P_COMMENT,P_MFGR','SORT_COLUMNS'='P_SIZE,P_TYPE,P_NAME,
          |  P_BRAND,P_CONTAINER')""".stripMargin)
    spark
      .sql(
        """create table if not exists LINEITEM(  L_SHIPDATE date,  L_SHIPMODE string,
          |L_SHIPINSTRUCT string,  L_RETURNFLAG string,  L_RECEIPTDATE date,  L_ORDERKEY INT ,
          |L_PARTKEY INT ,  L_SUPPKEY   string,  L_LINENUMBER int,  L_QUANTITY double,
          |L_EXTENDEDPRICE double,  L_DISCOUNT double,  L_TAX double,  L_LINESTATUS string,
          |L_COMMITDATE date,  L_COMMENT  string)
          |STORED BY 'org.apache.carbondata.format'
          |TBLPROPERTIES ('DICTIONARY_INCLUDE'='L_SHIPDATE,L_SHIPMODE,L_SHIPINSTRUCT,
          |L_RECEIPTDATE,L_COMMITDATE,L_RETURNFLAG,L_LINESTATUS','DICTIONARY_EXCLUDE'='L_SUPPKEY,
          | L_COMMENT', 'table_blocksize'='300', 'no_inverted_index'='L_SUPPKEY,L_COMMENT',
          | 'SORT_COLUMNS'='L_SHIPDATE,L_RETURNFLAG,L_SHIPMODE,L_RECEIPTDATE,L_SHIPINSTRUCT')"""
          .stripMargin)
    spark
      .sql(
        """create table if not exists ORDERS(  O_ORDERDATE date,  O_ORDERPRIORITY string,
          |O_ORDERSTATUS string,  O_ORDERKEY int,  O_CUSTKEY string,  O_TOTALPRICE double,
          |O_CLERK string,  O_SHIPPRIORITY int,  O_COMMENT string)
          | STORED BY 'org.apache.carbondata.format'
          | TBLPROPERTIES ('DICTIONARY_INCLUDE'='O_ORDERDATE,O_ORDERSTATUS',
          |'DICTIONARY_EXCLUDE'='O_ORDERPRIORITY, O_CUSTKEY, O_CLERK, O_COMMENT',
          |'table_blocksize'='300','no_inverted_index'='O_ORDERPRIORITY, O_CUSTKEY, O_CLERK,
          |O_COMMENT', 'SORT_COLUMNS'='O_ORDERDATE')""".stripMargin)
  }

  private val base = "/Users/jacky/code/tpch-osx/dbgen"

  private def loadData(spark: SparkSession) = {
    spark
      .sql(
        s"""load data inpath "$base/lineitem.tbl" into table lineitem
           | options('DELIMITER'='|','FILEHEADER'='L_ORDERKEY,L_PARTKEY,L_SUPPKEY,L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,L_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/supplier.tbl" into table SUPPLIER
          | options('DELIMITER'='|','FILEHEADER'='S_SUPPKEY,S_NAME,S_ADDRESS,S_NATIONKEY,S_PHONE,S_ACCTBAL,S_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/partsupp.tbl" into table PARTSUPP
           | options('DELIMITER'='|','FILEHEADER'='PS_PARTKEY,PS_SUPPKEY,PS_AVAILQTY,PS_SUPPLYCOST,PS_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/customer.tbl" into  table CUSTOMER
           | options('DELIMITER'='|','FILEHEADER'='C_CUSTKEY,C_NAME,C_ADDRESS,C_NATIONKEY,C_PHONE,C_ACCTBAL,C_MKTSEGMENT,C_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/nation.tbl"
          |into table NATION options('DELIMITER'='|','FILEHEADER'='N_NATIONKEY,N_NAME,N_REGIONKEY,N_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/region.tbl"
          |into table REGION options('DELIMITER'='|','FILEHEADER'='R_REGIONKEY,R_NAME,R_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/part.tbl" into
          |table PART options('DELIMITER'='|','FILEHEADER'='P_PARTKEY,P_NAME,P_MFGR,P_BRAND,P_TYPE,P_SIZE,P_CONTAINER,P_RETAILPRICE,P_COMMENT')""".stripMargin)
    spark
      .sql(
        s"""load data inpath "$base/orders.tbl"
          |into table ORDERS options('DELIMITER'='|','FILEHEADER'='O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT')""".stripMargin)
  }

  private def runQuery(spark: SparkSession): Unit = {
    spark.sql("""use tpchcarbon_default""")
    spark.sql(
      """
        |select l_returnflag, l_linestatus, sum(l_quantity) as sum_qty, sum(l_extendedprice) as
        |sum_base_price, sum(l_extendedprice*(1-l_discount)) as sum_disc_price, sum
        |(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty,
        |avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc, count(*) as count_order
        |from lineitem where l_shipdate <= date('1998-09-02') group by l_returnflag, l_linestatus
        | order by l_returnflag, l_linestatus
      """.stripMargin).show
  }
}
