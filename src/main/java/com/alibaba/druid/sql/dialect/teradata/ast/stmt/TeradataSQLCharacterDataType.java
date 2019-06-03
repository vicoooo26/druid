package com.alibaba.druid.sql.dialect.teradata.ast.stmt;

import com.alibaba.druid.sql.ast.statement.SQLCharacterDataType;

public class TeradataSQLCharacterDataType extends SQLCharacterDataType {

  private boolean caseSensitive = false;
  public TeradataSQLCharacterDataType(String name){
    super(name);
  }

  public TeradataSQLCharacterDataType(String name, int precision){
    super(name, precision);
  }

  public boolean isCaseSensitive() {
    return caseSensitive;
  }

  public void setCaseSensitive(boolean caseSensitive) {
    this.caseSensitive = caseSensitive;
  }
}
