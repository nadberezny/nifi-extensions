package pl.touk.nifi.services.lookup.sql;

import java.util.Objects;

public class LookupSqlArgument {
    private int ix;
    private Object value;

    public LookupSqlArgument(int ix, Object value) {
        this.ix = ix;
        this.value = value;
    }

    public int getIx() {
        return ix;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public boolean equals(Object otherObj) {
        if (this == otherObj) return true;
        if (otherObj == null || getClass() != otherObj.getClass()) return false;
        LookupSqlArgument other = (LookupSqlArgument) otherObj;
        return ix == other.ix && Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ix, value);
    }
}