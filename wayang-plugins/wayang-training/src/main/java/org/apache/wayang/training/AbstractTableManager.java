package org.apache.wayang.planGeneration;

import java.util.HashMap;

public abstract class AbstractTableManager {
  String tableName;
  String typeSchema;
  HashMap<String, String> fields;
}
