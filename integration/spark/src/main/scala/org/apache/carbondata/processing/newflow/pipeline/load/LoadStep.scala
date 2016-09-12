package org.apache.carbondata.processing.newflow.pipeline.load

import org.apache.carbondata.processing.newflow.pipeline.{PipelineContext, Step}
import org.apache.spark.sql.{Row, SQLContext}

class LoadStep extends Step {
  override def doWork(context: PipelineContext): Unit = {

    val sqlContext = context.get("sc").asInstanceOf[SQLContext]
    val path = context.get("path").asInstanceOf[String]
    val format = context.get("format").asInstanceOf[String]
    val option = context.get("option").asInstanceOf[Map[String, String]]

    // read input file and generate dictionary
    val input = sqlContext.read
        .format(format)
        .options(option)
        .load(path)

    val mapFunc = new DictGenMapper(3)
    val writeFunc = new DictGenReducer
    input.map(mapFunc.apply)
        .reduceByKey((s1, s2) => s1.union(s2))
        .mapValues(writeFunc.apply)
  }
}


class LoadStepFactory extends Step.Factory {
  override def create(): Step = new LoadStep
}

// TODO: need a way to reuse following class across computation framework

class ColumnarFileWriter(codecs: Map[String, Codec]) {
  // codec for each column
  def apply(row: Row): Unit = {
    // TODO: write to CarbonData file using corresponding column codec
  }
}

class Codec {}