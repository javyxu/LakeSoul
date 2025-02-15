/*
 * Copyright [2022] [DMetaSoul Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.lakesoul.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class LakeSoulStaticSplitEnumerator implements SplitEnumerator<LakeSoulSplit, LakeSoulPendingSplits> {

    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulStaticSplitEnumerator.class);

    private final SplitEnumeratorContext<LakeSoulSplit> context;

    private final LakeSoulSimpleSplitAssigner splitAssigner;

    public LakeSoulStaticSplitEnumerator(SplitEnumeratorContext<LakeSoulSplit> context,
                                         LakeSoulSimpleSplitAssigner splitAssigner) {
        this.context = context;
        this.splitAssigner = splitAssigner;
    }

    @Override
    public void start() {}

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        final Optional<LakeSoulSplit> nextSplit = splitAssigner.getNext();
        if (nextSplit.isPresent()) {
            final LakeSoulSplit split = nextSplit.get();
            context.assignSplit(split, subtaskId);
            LOG.info("Assigned split to subtask {} : {}", subtaskId, split);
        } else {
            context.signalNoMoreSplits(subtaskId);
            LOG.info("No more splits available for subtask {}", subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<LakeSoulSplit> splits, int subtaskId) {
        LOG.info("Add split back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {}

    @Override
    public LakeSoulPendingSplits snapshotState(long checkpointId) throws Exception {
        LOG.info("LakeSoulStaticSplitEnumerator snapshotState");
        return null;
    }

    @Override
    public void close() throws IOException {}
}
