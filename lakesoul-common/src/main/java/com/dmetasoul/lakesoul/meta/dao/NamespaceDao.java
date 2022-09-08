package com.dmetasoul.lakesoul.meta.dao;

import com.dmetasoul.lakesoul.meta.DBConnector;
import com.dmetasoul.lakesoul.meta.DBUtil;
import com.dmetasoul.lakesoul.meta.entity.Namespace;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class NamespaceDao {
    public boolean insert(Namespace namespace) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        boolean result = true;
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement("insert into namespace(name, properties) " +
                    "values (?, ?)");
            pstmt.setString(1, namespace.getName());
            pstmt.setString(2, DBUtil.jsonToString(namespace.getProperties()));
            pstmt.execute();
        } catch (SQLException e) {
            result = false;
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
        return result;
    }

    public void deleteByName(String name) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        String sql = String.format("delete from namespace where name = '%s' ", name);
        try {
            conn = DBConnector.getConn();
            pstmt = conn.prepareStatement(sql);
            pstmt.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DBConnector.closeConn(pstmt, conn);
        }
    }
}
