/*
 *
 *  * Copyright [2022] [DMetaSoul Team]
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.sink.writer;

import com.dmetasoul.lakesoul.lakesoul.io.NativeIOWriter;
import com.dmetasoul.lakesoul.lakesoul.memory.ArrowMemoryUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.lakesoul.tool.FlinkUtil;
import org.apache.flink.lakesoul.tool.LakeSoulSinkOptions;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.arrow.ArrowUtils;
import org.apache.flink.table.runtime.arrow.ArrowWriter;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BINLOG_FILE_INDEX;
import static org.apache.flink.lakesoul.tool.LakeSoulSinkOptions.BINLOG_POSITION;

public class NativeParquetWriter implements InProgressFileWriter<RowData, String> {

    private final ArrowWriter<RowData> arrowWriter;

    private NativeIOWriter nativeWriter;

    private final int batchSize;

    private final long creationTime;

    private final VectorSchemaRoot batch;

    private final String bucketID;

    private int rowsInBatch;

    long lastUpdateTime;

    String path;

    protected BufferAllocator allocator;

    public NativeParquetWriter(RowType rowType,
                               List<String> primaryKeys,
                               String bucketID,
                               Path path,
                               long creationTime,
                               Configuration conf) throws IOException {
        this.batchSize = 250000; // keep same with native writer's row group row number
        this.creationTime = creationTime;
        this.bucketID = bucketID;
        this.rowsInBatch = 0;
        this.allocator = ArrowMemoryUtils.rootAllocator.newChildAllocator("NativeParquetWriter", 0, Long.MAX_VALUE);

        Schema arrowSchema = ArrowUtils.toArrowSchema(rowType);
        nativeWriter = new NativeIOWriter(arrowSchema);
        nativeWriter.setPrimaryKeys(primaryKeys);
        if (conf.getBoolean(LakeSoulSinkOptions.isMultiTableSource) == true) {
            nativeWriter.setAuxSortColumns(Arrays.asList(BINLOG_FILE_INDEX, BINLOG_POSITION));
        }
        nativeWriter.setRowGroupRowNumber(this.batchSize);
        batch = VectorSchemaRoot.create(arrowSchema, this.allocator);
        arrowWriter = ArrowUtils.createRowDataArrowWriter(batch, rowType);
        this.path = path.makeQualified(path.getFileSystem()).toString();
        nativeWriter.addFile(this.path);

        FlinkUtil.setFSConfigs(conf, this.nativeWriter);
        this.nativeWriter.initializeWriter();
    }

    @Override
    public void write(RowData element, long currentTime) throws IOException {
        this.lastUpdateTime = currentTime;
        this.arrowWriter.write(element);
        this.rowsInBatch++;
        if (this.rowsInBatch >= this.batchSize) {
            this.arrowWriter.finish();
            this.nativeWriter.write(this.batch);
            // in native writer, batch may be kept in memory for sorting,
            // so we have to release ownership in java
            this.batch.clear();
            this.arrowWriter.reset();
            this.rowsInBatch = 0;
        }
    }

    @Override
    public InProgressFileRecoverable persist() throws IOException {
        // we currently do not support persist
        return null;
    }

    static public class NativeWriterPendingFileRecoverable implements PendingFileRecoverable, Serializable {
        public String path;

        public long creationTime;

        NativeWriterPendingFileRecoverable(String path, long creationTime) {
            this.path = path;
            this.creationTime = creationTime;
        }

        @Override
        public String toString() {
            return "PendingFile(" +
                    path + ", " + creationTime + ")";
        }
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        this.arrowWriter.finish();
        this.nativeWriter.write(this.batch);
        this.nativeWriter.flush();
        this.arrowWriter.reset();
        this.rowsInBatch = 0;
        this.batch.clear();
        this.batch.close();
        this.allocator.close();
        try {
            this.nativeWriter.close();
            this.nativeWriter = null;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new NativeWriterPendingFileRecoverable(this.path, this.creationTime);
    }

    @Override
    public void dispose() {
        try {
            this.nativeWriter.close();
            this.nativeWriter = null;
            this.arrowWriter.finish();
            this.batch.close();
            this.allocator.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getBucketId() {
        return this.bucketID;
    }

    @Override
    public long getCreationTime() {
        return this.creationTime;
    }

    @Override
    public long getSize() throws IOException {
        return 0;
    }

    @Override
    public long getLastUpdateTime() {
        return this.lastUpdateTime;
    }
}
