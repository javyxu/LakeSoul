package org.apache.spark.sql.execution.datasources.v2.parquet.Native;

import org.apache.arrow.lakesoul.io.ArrowCDataWrapper;
import org.apache.arrow.lakesoul.io.read.LakeSoulArrowReader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.spark.internal.Logging;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile;
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.OffHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ArrowUtils;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import scala.Function0;

import java.io.Closeable;
import java.io.IOException;

public class NativeVectorizedReader implements AutoCloseable {

  // The capacity of vectorized batch.
  private int capacity;
  private ArrowCDataWrapper wrapper;
  private LakeSoulArrowReader reader;

  private WritableColumnVector[] partitionColumnVectors;

  /**
   * The memory mode of the columnarBatch
   */
  private final MemoryMode MEMORY_MODE;

  public NativeVectorizedReader(MergePartitionedFile[] files, StructType partitionSchema, int capacity){
    wrapper=new ArrowCDataWrapper();
    wrapper.initializeConfigBuilder();
    wrapper.addFile(files[0].filePath());

    wrapper.setThreadNum(2);
    wrapper.createReader();
    wrapper.startReader(bool -> {});
    reader = new LakeSoulArrowReader(wrapper);

    MEMORY_MODE = MemoryMode.ON_HEAP;
    initializePartitionColumns(MEMORY_MODE, partitionSchema, files[0].partitionValues());
    this.capacity = capacity;
  }

  public boolean nextKeyValue() throws IOException {
    return reader.hasNext();
  }

  public ColumnarBatch getCurrentValue() {
    VectorSchemaRoot vsr = reader.nextResultVectorSchemaRoot();
    return new ColumnarBatch(concatBatchVectorWithPartitionVectors(ArrowUtils.asArrayColumnVector(vsr)), capacity);
  }

  // partition columns appended to the end of the batch.
  // For example, if the data contains two columns, with 2 partition columns:
  // Columns 0,1: data columns
  // Column 2: partitionValues[0]
  // Column 3: partitionValues[1]
  public void initializePartitionColumns(
          MemoryMode memMode,
          StructType partitionColumns,
          InternalRow partitionValues) {
    System.out.println("[Debug][huazeng]on initializePartitionColumns");
    System.out.println("[Debug][huazeng]on initializePartitionColumns, partitionValues:"+partitionValues.toString());
    StructType partitionSchema = new StructType();
    if (partitionColumns != null) {
      for (StructField f : partitionColumns.fields()) {
        partitionSchema = partitionSchema.add(f);
      }
    }
    if (memMode == MemoryMode.OFF_HEAP) {
      partitionColumnVectors = OffHeapColumnVector.allocateColumns(capacity, partitionSchema);
    } else {
      partitionColumnVectors = OnHeapColumnVector.allocateColumns(capacity, partitionSchema);
    }
    if (partitionColumns != null) {
      for (int i = 0; i < partitionColumns.fields().length; i++) {
        ColumnVectorUtils.populate(partitionColumnVectors[i], partitionValues, 0);
        partitionColumnVectors[i].setIsConstant();
      }
    }
    System.out.println("[Debug][huazeng]on initializePartitionColumns: partitionColumnVectors.length=" + partitionColumnVectors.length);
    System.out.println(partitionColumnVectors.toString());
  }

  private ColumnVector[] concatBatchVectorWithPartitionVectors(ColumnVector[] batchVectors){
    ColumnVector[] descColumnVectors = new ColumnVector[batchVectors.length + partitionColumnVectors.length];
    System.arraycopy(batchVectors, 0, descColumnVectors, 0, batchVectors.length);
    System.arraycopy(partitionColumnVectors, 0, descColumnVectors, partitionColumnVectors.length, partitionColumnVectors.length);
    System.out.println("[Debug][huazeng]on concatBatchVectorWithPartitionVectors");
    System.out.println(descColumnVectors.length);
    return descColumnVectors;
  }


  /**
   * Closes this resource, relinquishing any underlying resources.
   * This method is invoked automatically on objects managed by the
   * {@code try}-with-resources statement.
   *
   * @throws Exception if this resource cannot be closed
   * @apiNote While this interface method is declared to throw {@code
   * Exception}, implementers are <em>strongly</em> encouraged to
   * declare concrete implementations of the {@code close} method to
   * throw more specific exceptions, or to throw no exception at all
   * if the close operation cannot fail.
   *
   * <p> Cases where the close operation may fail require careful
   * attention by implementers. It is strongly advised to relinquish
   * the underlying resources and to internally <em>mark</em> the
   * resource as closed, prior to throwing the exception. The {@code
   * close} method is unlikely to be invoked more than once and so
   * this ensures that the resources are released in a timely manner.
   * Furthermore it reduces problems that could arise when the resource
   * wraps, or is wrapped, by another resource.
   *
   * <p><em>Implementers of this interface are also strongly advised
   * to not have the {@code close} method throw {@link
   * InterruptedException}.</em>
   * <p>
   * This exception interacts with a thread's interrupted status,
   * and runtime misbehavior is likely to occur if an {@code
   * InterruptedException} is {@linkplain Throwable#addSuppressed
   * suppressed}.
   * <p>
   * More generally, if it would cause problems for an
   * exception to be suppressed, the {@code AutoCloseable.close}
   * method should not throw it.
   *
   * <p>Note that unlike the {@link Closeable#close close}
   * method of {@link Closeable}, this {@code close} method
   * is <em>not</em> required to be idempotent.  In other words,
   * calling this {@code close} method more than once may have some
   * visible side effect, unlike {@code Closeable.close} which is
   * required to have no effect if called more than once.
   * <p>
   * However, implementers of this interface are strongly encouraged
   * to make their {@code close} methods idempotent.
   */
  @Override
  public void close() throws Exception {

  }
}
