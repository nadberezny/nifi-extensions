package pl.touk.nifi.services.lookup.sql;

import pl.touk.nifi.common.ThrowingConsumer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

public class LookupSqlQuery {
    private String tableName;
    private String whereClause;
    private Set<String> selectColumns;
    private Set<LookupSqlArgument> args;

    public LookupSqlQuery(Set<String> selectColumns, String tableName, String whereClause, Set<LookupSqlArgument> args) {
        this.selectColumns = selectColumns;
        this.tableName = tableName;
        this.whereClause = whereClause;
        this.args = args;
    }

    public PreparedStatement toStatement(Connection conn) throws SQLException {
        PreparedStatement st = conn.prepareStatement(queryStr());
        args.forEach(
                consumeWithSqlExceptionHandle(
                        arg -> st.setObject(arg.getIx(), arg.getValue())));
        return st;
    }

    private String queryStr() {
        return "SELECT " + selectClause() + " FROM " + tableName + " WHERE " + whereClause;
    }

    private String selectClause() {
        return selectColumns.isEmpty() ? "*" : String.join(",", selectColumns);
    }

    @Override
    public boolean equals(Object otherObj) {
        if (this == otherObj) return true;
        if (otherObj == null || getClass() != otherObj.getClass()) return false;
        LookupSqlQuery other = (LookupSqlQuery) otherObj;
        return Objects.equals(selectColumns, other.selectColumns) &&
               Objects.equals(tableName, other.tableName) &&
               Objects.equals(whereClause, other.whereClause) &&
               Objects.equals(args, other.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(selectColumns, tableName, whereClause, args);
    }

    private static <A> Consumer<A> consumeWithSqlExceptionHandle(ThrowingConsumer<A, SQLException> consumer) {
        return a -> {
            try {
                consumer.accept(a);
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}