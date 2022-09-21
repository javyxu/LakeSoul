package com.dmetasoul.lakesoul.meta.external;

import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabaseSchemaedTables {
    private String DBName;
    HashMap<String, Table> tables = new HashMap<>(20);

    public DatabaseSchemaedTables(String DBName) {
        this.DBName = DBName;
    }

    public Table addTable(String tableName) {
        if (!tables.containsKey(tableName)) {
            Table tb = new Table(tableName);
            tables.put(tableName, tb);
        }
        return tables.get(tableName);
    }

    public List<Table> getTables(){
        return new ArrayList<>(tables.values());
    }

    public class Table {
        String TName;
        List<Column> cols = new ArrayList<>(50);
        List<PK> PKs = new ArrayList<>(10);

        public Table(String TName) {
            this.TName = TName;
        }

        public void addColumn(String colName, String colType) {
            cols.add(new Column(colName, colType));
        }

        public void addPrimaryKey(String key, int index) {
            if (key != null) {
                PKs.add(new PK(key, index));
            }
        }
        

        @Override
        public String toString() {
            return "Table{" +
                    "TName='" + TName + '\'' +
                    ", cols=" + cols +
                    ", PKs=" + PKs +
                    '}';
        }
    }

    class Column {
        String colName;
        String type;

        public Column(String colName, String type) {
            this.colName = colName;
            this.type = type;
        }

        @Override
        public String toString() {
            return "Column{" +
                    "colName='" + colName + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    class PK implements Comparable<PK> {
        String colName;
        int pos;

        public PK(String colName, int pos) {
            this.colName = colName;
            this.pos = pos;
        }

        @Override
        public int compareTo(PK o) {
            return this.pos - o.pos;
        }

        @Override
        public String toString() {
            return "PK{" +
                    "colName='" + colName + '\'' +
                    ", pos=" + pos +
                    '}';
        }
    }

    @Override
    public String toString() {
        return "DatabaseSchemaedTables{" +
                "DBName='" + DBName + '\'' +
                ", tables=" + tables +
                '}';
    }
}