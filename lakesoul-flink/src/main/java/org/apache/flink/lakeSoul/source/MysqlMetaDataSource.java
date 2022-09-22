package org.apache.flink.lakeSoul.source;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class MysqlMetaDataSource implements JdbcMetaDataSource {

    HashSet<String> excludeTables;
    private HikariConfig config = new HikariConfig();
    private HikariDataSource ds;
    private String databaseName;
    private String[] filterTables = new String[]{"sys_config"};

    public MysqlMetaDataSource(String DBName, String user, String passwd, String host, String port, HashSet<String> excludeTables) {
        this.excludeTables = excludeTables;
        excludeTables.addAll(Arrays.asList(filterTables));
        this.databaseName = DBName;
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        String url = "jdbc:mysql://" + host + ":" + port + "/" + DBName + "?useSSL=false";
        config.setJdbcUrl(url);
        config.setUsername(user);
        config.setPassword(passwd);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        ds = new HikariDataSource(this.config);
    }

    @Override
    public DatabaseSchemaedTables getDatabaseAndTablesWithSchema() {
        Connection connection = null;
        DatabaseSchemaedTables dct = new DatabaseSchemaedTables(this.databaseName);
        try {
            connection = ds.getConnection();
            DatabaseMetaData dmd = connection.getMetaData();
            ResultSet tables = dmd.getTables(null, null, null, new String[]{"TABLE"});
            while (tables.next()) {
                String tablename = tables.getString("TABLE_NAME");
                if (excludeTables.contains(tablename)) {
                    continue;
                }
                DatabaseSchemaedTables.Table tbl = dct.addTable(tablename);
                ResultSet cols = dmd.getColumns(null, null, tablename, null);
                while (cols.next()) {
                    System.out.println(cols.getString("COLUMN_NAME")+" "+cols.getString("TYPE_NAME")+" "+cols.getString("COLUMN_SIZE"));
                    tbl.addColumn(cols.getString("COLUMN_NAME"), cols.getString("TYPE_NAME"));
                }
                ResultSet pks = dmd.getPrimaryKeys(null, null, tablename);
                while (pks.next()) {
                    tbl.addPrimaryKey(pks.getString("COLUMN_NAME"), pks.getShort("KEY_SEQ"));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            ds.close();
        }

        return dct;
    }


}
