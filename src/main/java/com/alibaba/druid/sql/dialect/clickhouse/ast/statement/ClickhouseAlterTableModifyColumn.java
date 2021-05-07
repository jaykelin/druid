package com.alibaba.druid.sql.dialect.clickhouse.ast.statement;

import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLObjectImpl;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

/**
 * @author jaykelin
 * @version 2021/4/29 6:27 下午
 */
public class ClickhouseAlterTableModifyColumn extends SQLObjectImpl implements SQLAlterTableItem {

    private SQLName column;


    @Override
    protected void accept0(SQLASTVisitor visitor) {

    }
}
