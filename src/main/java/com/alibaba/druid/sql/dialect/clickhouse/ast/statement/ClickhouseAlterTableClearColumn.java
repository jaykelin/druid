package com.alibaba.druid.sql.dialect.clickhouse.ast.statement;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

/**
 * @author jaykelin
 * @version 2021/4/29 4:48 下午
 */
public class ClickhouseAlterTableClearColumn extends SQLObjectImpl implements SQLAlterTableItem {

    private SQLName column;
    private SQLName partition;

    public ClickhouseAlterTableClearColumn() {}

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, column);
            acceptChild(visitor, partition);
        }
        visitor.endVisit(this);
    }

    public SQLName getColumn() {
        return column;
    }

    public void setColumn(SQLName column) {
        if (column != null) {
            column.setParent(this);
        }
        this.column = column;
    }

    public SQLName getPartition() {
        return partition;
    }

    public void setPartition(SQLName partition) {
        if (partition != null) {
            partition.setParent(this);
        }
        this.partition = partition;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
