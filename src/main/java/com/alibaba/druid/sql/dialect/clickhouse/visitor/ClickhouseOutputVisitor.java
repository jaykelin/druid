package com.alibaba.druid.sql.dialect.clickhouse.visitor;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.ast.*;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.dialect.clickhouse.ast.statement.ClickhouseAlterTableClearColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableRenameColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.dialect.clickhouse.ast.ClickhouseCreateTableStatement;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;

import java.util.List;

public class ClickhouseOutputVisitor extends SQLASTOutputVisitor implements ClickhouseVisitor {
    public ClickhouseOutputVisitor(Appendable appender) {
        super(appender);
    }

    public ClickhouseOutputVisitor(Appendable appender, DbType dbType) {
        super(appender, dbType);
    }

    public ClickhouseOutputVisitor(Appendable appender, boolean parameterized) {
        super(appender, parameterized);
    }

    @Override
    public boolean visit(SQLWithSubqueryClause.Entry x) {
        if (x.getExpr() != null) {
            x.getExpr().accept(this);
        } else if (x.getSubQuery() != null) {
            print('(');
            println();
            SQLSelect query = x.getSubQuery();
            if (query != null) {
                query.accept(this);
            } else {
                x.getReturningStatement().accept(this);
            }
            println();
            print(')');
        }
        print(' ');
        print0(ucase ? "AS " : "as ");
        print0(x.getAlias());

        return false;
    }

    public boolean visit(SQLStructDataType x) {
        print0(ucase ? "NESTED (" : "nested (");
        incrementIndent();
        println();
        printlnAndAccept(x.getFields(), ",");
        decrementIndent();
        println();
        print(')');
        return false;
    }

    @Override
    public boolean visit(SQLStructDataType.Field x) {
        SQLName name = x.getName();
        if (name != null) {
            name.accept(this);
        }
        SQLDataType dataType = x.getDataType();

        if (dataType != null) {
            print(' ');
            dataType.accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(ClickhouseCreateTableStatement x) {
        super.visit((SQLCreateTableStatement) x);

        SQLExpr partitionBy = x.getPartitionBy();
        if (partitionBy != null) {
            println();
            print0(ucase ? "PARTITION BY " : "partition by ");
            partitionBy.accept(this);
        }

        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            println();
            orderBy.accept(this);
        }

        SQLExpr sampleBy = x.getSampleBy();
        if (sampleBy != null) {
            println();
            print0(ucase ? "SAMPLE BY " : "sample by ");
            sampleBy.accept(this);
        }

        List<SQLAssignItem> settings = x.getSettings();
        if (!settings.isEmpty()) {
            println();
            print0(ucase ? "SETTINGS " : "settings ");
            printAndAccept(settings, ", ");
        }
        return false;
    }

    @Override
    public boolean visit(SQLAlterTableStatement x) {
        print0(ucase ? "ALTER TABLE " : "alter table ");
        printTableSourceExpr(x.getName());

        if (x.isOnCluster()) {
            print0(ucase ? " ON CLUSTER " : " on cluster ");
            print0(x.getCluster().getSimpleName());
        }

        for (SQLAlterTableItem item : x.getItems()){
            if (item instanceof SQLAlterTableDropColumnItem) {
                print0(ucase ? " DROP COLUM " : " drop column ");
                if (x.isIfExists()) {
                    print0(ucase ? " IF EXISTS ": " if exists ");
                }

                print0(((SQLAlterTableDropColumnItem) item).getColumns().get(0).getSimpleName());
            } else if (item instanceof SQLAlterTableAddColumn) {
                print0(ucase ? " ADD COLUMN " : " add column ");
                if (x.isIfExists()) {
                    print0(ucase ? "IF NOT EXISTS " : "if not exists ");
                }
                //print0(((SQLAlterTableAddColumn) item).getAfterColumn().getSimpleName());
                SQLColumnDefinition columnDefinition = ((SQLAlterTableAddColumn) item).getColumns().get(0);
                print0(columnDefinition.toString());
                if (((SQLAlterTableAddColumn) item).getAfterColumn() != null) {
                    print0(ucase ? " AFTER " : " after ");
                    print0(((SQLAlterTableAddColumn) item).getAfterColumn().getSimpleName());

                } else {
                    if (((SQLAlterTableAddColumn) item).isFirst()) {
                        print0(ucase ? " FIRST": "first");
                    }
                }
            } else if (item instanceof SQLAlterTableRenameColumn) {
                print0(ucase ? " RENAME COLUMN " : " rename column ");

                if (x.isIfExists()) {
                    print0(ucase ? "IF EXISTS " : "if exists ");
                }

                print0(((SQLAlterTableRenameColumn) item).getColumn().getSimpleName());
                print0(ucase ? " TO " : " to ");
                print0(((SQLAlterTableRenameColumn) item).getTo().getSimpleName());
            } else if (item instanceof ClickhouseAlterTableClearColumn) {
                print0(ucase ? " CLEAR COLUMN " : " clear column ");

                if (x.isIfExists()) {
                    print0(ucase ? "IF EXISTS " : "if exists ");
                }

                print0(((ClickhouseAlterTableClearColumn) item).getColumn().getSimpleName());

                print0(ucase ? " IN PARTITION " : " in partition ");
                print0(((ClickhouseAlterTableClearColumn) item).getPartition().getSimpleName());
            }
        }
        return false;
    }
}
