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

package org.apache.wayang.benchmarks.job.complex;

import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Arrays;
import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/*
 * SELECT
 *   MIN(cn.name) AS company_name,
 *   MIN(lt.link) AS link_type,
 *   MIN(t.title) AS western_follow_up
 * FROM
 *   company_name AS cn,
 *   company_type AS ct,
 *   keyword AS k,
 *   link_type AS lt,
 *   movie_companies AS mc,
 *   movie_info AS mi,
 *   movie_keyword AS mk,
 *   movie_link AS ml,
 *   title AS t
 * WHERE
 *   cn.country_code !='[pl]'
 *   AND (cn.name LIKE '%Film%' OR cn.name LIKE '%Warner%')
 *   AND ct.kind ='production companies'
 *   AND k.keyword ='sequel'
 *   AND lt.link LIKE '%follow%'
 *   AND mc.note IS NULL
 *   AND mi.info IN ('Sweden', 'Norway', 'Germany', 'Denmark')
 *   AND t.production_year BETWEEN 1950 AND 2000
 *   AND lt.id = ml.link_type_id
 *   AND ml.movie_id = t.id
 *   AND t.id = mk.movie_id
 *   AND mk.keyword_id = k.id
 *   AND t.id = mc.movie_id
 *   AND mc.company_type_id = ct.id
 *   AND mc.company_id = cn.id
 *   AND ml.movie_id = mk.movie_id
 *   AND mk.movie_id = mc.movie_id
 *   AND ml.movie_id = mi.movie_id
 *   AND mc.movie_id = mi.movie_id
 *   AND cn.name_pcode_nf = cn.name_pcode_sf
 *   ANe mi.movie_id = t.id
 *   AND ml.movie_id = mc.movie_id
 *   AND mk.movie_id = mi.movie_id;
 */
public class Query1 {

    public static WayangPlan getWayangPlan(String dataPath, Collection<?> collector){
        return null;
    }
}
