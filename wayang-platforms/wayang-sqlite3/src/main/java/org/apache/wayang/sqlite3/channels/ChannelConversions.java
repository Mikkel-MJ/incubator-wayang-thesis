/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.sqlite3.channels;

import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.optimizer.channels.DefaultChannelConversion;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.jdbc.operators.SqlToRddOperator;
import org.apache.wayang.jdbc.operators.SqlToStreamOperator;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Register for the {@link ChannelConversion}s supported for this platform.
 */
public class ChannelConversions {

    public static final ChannelConversion SQL_TO_STREAM_CONVERSION = new DefaultChannelConversion(
            Sqlite3Platform.getInstance().getSqlQueryChannelDescriptor(),
            StreamChannel.DESCRIPTOR,
            () -> new SqlToStreamOperator(
              Sqlite3Platform.getInstance(),
              DataSetType.createDefault(Object.class),
              DataSetType.createDefault(Object.class)
            )
    );

    public static final ChannelConversion SQL_TO_UNCACHED_RDD_CONVERSION = new DefaultChannelConversion(
            Sqlite3Platform.getInstance().getSqlQueryChannelDescriptor(),
            RddChannel.UNCACHED_DESCRIPTOR,
            () -> new SqlToRddOperator(Sqlite3Platform.getInstance())
    );

    public static final Collection<ChannelConversion> ALL = Arrays.asList(
            SQL_TO_STREAM_CONVERSION,
            SQL_TO_UNCACHED_RDD_CONVERSION
    );

}
