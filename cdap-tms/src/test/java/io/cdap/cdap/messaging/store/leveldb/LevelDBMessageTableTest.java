/*
 * Copyright © 2016-2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.messaging.store.leveldb;

import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.messaging.store.MessageTable;
import io.cdap.cdap.messaging.store.MessageTableTest;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.messaging.store.TestMessageEntry;
import io.cdap.cdap.proto.id.TopicId;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link LevelDBMessageTable}.
 */
public class LevelDBMessageTableTest extends MessageTableTest {
  private static final int PARTITION_SECONDS = 10;

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  protected static CConfiguration cConf;
  private static TableFactory tableFactory;

  @BeforeClass
  public static void init() throws IOException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tmpFolder.newFolder().getAbsolutePath());
    cConf.set(Constants.MessagingSystem.LOCAL_DATA_PARTITION_SECONDS, Integer.toString(PARTITION_SECONDS));
    tableFactory = new LevelDBTableFactory(cConf);
  }

  @Override
  protected MessageTable getMessageTable(TopicMetadata topicMetadata) throws Exception {
    return tableFactory.createMessageTable(topicMetadata);
  }

  @Override
  protected MetadataTable getMetadataTable() throws Exception {
    return tableFactory.createMetadataTable();
  }

  @Test
  public void testUpgrade() throws Exception {
    File baseDir = tmpFolder.newFolder();
    String tableName = "messages";
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.MessagingSystem.LOCAL_DATA_DIR, baseDir.getAbsolutePath());
    cConf.set(Constants.MessagingSystem.MESSAGE_TABLE_NAME, tableName);
    cConf.set(Constants.MessagingSystem.LOCAL_DATA_PARTITION_SECONDS, Integer.toString(1));
    LevelDBTableFactory tableFactory = new LevelDBTableFactory(cConf);

    TopicId topicId = new TopicId("system", "upgrade-test");
    int generation = 1;
    TopicMetadata topicMetadata =
      new TopicMetadata(topicId, Collections.singletonMap(TopicMetadata.GENERATION_KEY, String.valueOf(generation)));

    // write a message to a table, then rename the underlying directory to the old format
    long publishTime = 1000;
    try (MessageTable table = tableFactory.createMessageTable(topicMetadata)) {
      List<MessageTable.Entry> writes = new ArrayList<>();
      writes.add(new TestMessageEntry(topicId, generation, publishTime, 0, null, new byte[]{ 0 }));
      table.store(writes.iterator());
    }
    tableFactory.close();

    File topicDir = LevelDBTableFactory.getMessageTablePath(baseDir, topicId, generation, tableName);
    File partitionDir = LevelDBPartitionManager.getPartitionDir(topicDir, 0, publishTime + 1000);
    File oldDir = new File(baseDir,
                           topicDir.getName().substring(LevelDBTableFactory.MESSAGE_TABLE_VERSION.length() + 1));
    Files.move(partitionDir.toPath(), oldDir.toPath());

    // now run the upgrade and make sure the table is readable.
    tableFactory = new LevelDBTableFactory(cConf);
    tableFactory.init();

    try (MessageTable table = tableFactory.createMessageTable(topicMetadata)) {
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      try (CloseableIterator<MessageTable.Entry> iter =
             table.fetch(topicMetadata, new MessageId(messageId), true, 100, null)) {
        Assert.assertTrue(iter.hasNext());
        MessageTable.Entry entry = iter.next();
        Assert.assertEquals(publishTime, entry.getPublishTimestamp());
        Assert.assertFalse(iter.hasNext());
      }
    }
  }

  @Test
  public void testMultiPartitionReadWrite() throws Exception {
    TopicId topicId = new TopicId("default", "multipart");
    int generation = 1;
    TopicMetadata topicMetadata =
      new TopicMetadata(topicId, Collections.singletonMap(TopicMetadata.GENERATION_KEY, String.valueOf(generation)));

    try (MessageTable table = tableFactory.createMessageTable(topicMetadata)) {
      List<MessageTable.Entry> writes = new ArrayList<>();
      Map<Long, Byte> expected = new HashMap<>();
      for (int i = 0; i < 10 * PARTITION_SECONDS; i++) {
        long publishTime = i * 1000;
        expected.put(publishTime, (byte) i);
        writes.add(new TestMessageEntry(topicId, generation, publishTime, 0, null, new byte[]{(byte) i}));
      }
      table.store(writes.iterator());

      Map<Long, Byte> actual = new HashMap<>();
      byte[] messageId = new byte[MessageId.RAW_ID_SIZE];
      MessageId.putRawId(0L, (short) 0, 0L, (short) 0, messageId, 0);
      try (CloseableIterator<MessageTable.Entry> iter =
             table.fetch(topicMetadata, new MessageId(messageId), true, 100, null)) {
        while (iter.hasNext()) {
          MessageTable.Entry entry = iter.next();
          actual.put(entry.getPublishTimestamp(), entry.getPayload()[0]);
        }
      }

      Assert.assertEquals(expected, actual);
    }
  }
}
