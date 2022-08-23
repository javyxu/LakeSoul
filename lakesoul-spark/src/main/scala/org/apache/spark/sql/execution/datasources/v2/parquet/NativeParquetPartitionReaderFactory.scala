package org.apache.spark.sql.execution.datasources.v2.parquet
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderWithPartitionValues
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.execution.datasources.v2.parquet.Native.NativeVectorizedReader
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration



/**
  * A factory used to create Parquet readers.
  *
  * @param sqlConf         SQL configuration.
  * @param broadcastedConf Broadcast serializable Hadoop Configuration.
  * @param dataSchema      Schema of Parquet files.
  * @param readDataSchema  Required schema of Parquet files.
  * @param partitionSchema Schema of partitions.
  *                        //  * @param filterMap Filters to be pushed down in the batch scan.
  */
case class NativeParquetPartitionReaderFactory(sqlConf: SQLConf,
                                               broadcastedConf: Broadcast[SerializableConfiguration],
                                               dataSchema: StructType,
                                               readDataSchema: StructType,
                                               partitionSchema: StructType,
                                               filters: Array[Filter])
  extends NativeFilePartitionReaderFactory with Logging{
  override def buildReader(partitionedFile: PartitionedFile): PartitionReader[InternalRow] = ???

  def createVectorizedReader(file: PartitionedFile):NativeVectorizedReader = ???

  override def buildColumnarReader(file: PartitionedFile): PartitionReader[ColumnarBatch] = {
    val vectorizedReader = createVectorizedReader(file)

    new PartitionReader[ColumnarBatch] {
      override def next(): Boolean = vectorizedReader.nextKeyValue()

      override def get(): ColumnarBatch =
        vectorizedReader.getCurrentValue.asInstanceOf[ColumnarBatch]

      override def close(): Unit = vectorizedReader.close()
    }
  }
}
