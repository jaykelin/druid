package com.alibaba.druid.sql.dialect.clickhouse.parser;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.parser.*;
import com.alibaba.druid.util.FnvHash;

public class ClickhouseStatementParser extends SQLStatementParser {
    public ClickhouseStatementParser(String sql) {
        super (new ClickhouseExprParser(sql));
    }

    public ClickhouseStatementParser(String sql, SQLParserFeature... features) {
        super (new ClickhouseExprParser(sql, features));
    }

    public ClickhouseStatementParser(Lexer lexer){
        super(new ClickhouseExprParser(lexer));
    }


    @Override
    public SQLWithSubqueryClause parseWithQuery() {
        SQLWithSubqueryClause withQueryClause = new SQLWithSubqueryClause();
        if (lexer.hasComment() && lexer.isKeepComments()) {
            withQueryClause.addBeforeComment(lexer.readAndResetComments());
        }

        accept(Token.WITH);

        for (; ; ) {
            SQLWithSubqueryClause.Entry entry = new SQLWithSubqueryClause.Entry();
            entry.setParent(withQueryClause);

            if (lexer.token() == Token.LPAREN) {
                lexer.nextToken();
                switch (lexer.token()) {
                    case VALUES:
                    case WITH:
                    case SELECT:
                        entry.setSubQuery(
                                this.createSQLSelectParser()
                                        .select());
                        break;
                    default:
                        break;
                }
                accept(Token.RPAREN);

            } else {
                entry.setExpr(exprParser.expr());
            }

            accept(Token.AS);
            String alias = this.lexer.stringVal();
            lexer.nextToken();
            entry.setAlias(alias);

            withQueryClause.addEntry(entry);

            if (lexer.token() == Token.COMMA) {
                lexer.nextToken();
                continue;
            }

            break;
        }

        return withQueryClause;
    }

    @Override
    protected SQLAlterTableAddColumn parseAlterTableAddColumn() {

        SQLAlterTableAddColumn item = new SQLAlterTableAddColumn();

        SQLColumnDefinition columnDef = this.exprParser.parseColumn();
        item.addColumn(columnDef);

        if (lexer.token() == Token.FIRST) {
            lexer.nextToken();

            item.setFirst(true);
        }

        if (lexer.token() == Token.AFTER) {
            lexer.nextToken();
            item.setAfterColumn(this.exprParser.name());
        }
        return item;
    }

    @Override
    public SQLStatement parseAlter() {
        Lexer.SavePoint mark = lexer.mark();
        accept(Token.ALTER);

        if (lexer.token() == Token.TABLE) {
            lexer.nextToken();
            SQLAlterTableStatement stmt = new SQLAlterTableStatement(getDbType());

            stmt.setName(this.exprParser.name());

            if (lexer.token() == Token.ON) {
                lexer.nextToken();
                accept(Token.CLUSTER);

                stmt.setOnCluster(true);
                stmt.setCluster(this.exprParser.name());
            }

            for(;;){
                if (lexer.identifierEquals(FnvHash.Constants.ADD)) {
                    lexer.nextToken();

                    if (lexer.token() == Token.COLUMN) {
                        lexer.nextToken();
                        stmt.setIfExists(false);
                        if (lexer.token() == Token.IF) {
                            lexer.nextToken();
                            accept(Token.NOT);
                            accept(Token.EXISTS);
                            stmt.setIfExists(true);
                        }

                        SQLAlterTableAddColumn item = parseAlterTableAddColumn();

                        stmt.addItem(item);
                    }
                }
            }
        }



        return super.parseAlter();
    }

    public SQLCreateTableParser getSQLCreateTableParser() {
        return new ClickhouseCreateTableParser(this.exprParser);
    }
}
