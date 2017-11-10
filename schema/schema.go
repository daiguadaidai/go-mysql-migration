// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
    "database/sql"
    "fmt"
    "strings"

    "github.com/daiguadaidai/go-mysql-migration/mysql"
    "github.com/juju/errors"
)

var ErrTableNotExist = errors.New("table is not exist")

const (
    TYPE_NUMBER    = iota + 1 // tinyint, smallint, mediumint, int, bigint, year
    TYPE_FLOAT                // float, double
    TYPE_ENUM                 // enum
    TYPE_SET                  // set
    TYPE_STRING               // other
    TYPE_DATETIME             // datetime
    TYPE_TIMESTAMP            // timestamp
    TYPE_DATE                 // date
    TYPE_TIME                 // time
    TYPE_BIT                  // bit
    TYPE_JSON                 // json
)

type TableColumn struct {
    Name       string
    Type       int
    Collation  string
    RawType    string
    IsAuto     bool
    IsUnsigned bool
    EnumValues []string
    SetValues  []string
}

type Index struct {
    Name        string
    Columns     []string
    Cardinality []uint64
}

type Table struct {
    Schema string
    Name   string

    Columns   []TableColumn
    Indexes   []*Index
    PKColumns []int

    colNum         int    // 表列个数
    oldColNum      int    // 上一次表列个数
    insSQLTitTpl   string // Insert SQL Title 模板 REPLACE INTO %s(id, name) VALUES
    insSQLValTpl   string // Insert SQL Value 模板 (%s, %s)
    updSQLSetTpl   string // Update SQL 模板 UPDATE %s SET=%s
    updSQLWhereTpl string // Update SQL 模板 WHERE pri=%s
    delSQLTpl      string // Delete SQL 模板 DELETE FROM %s WHERE pri=%s
}

func (ta *Table) String() string {
    return fmt.Sprintf("%s.%s", ta.Schema, ta.Name)
}

func (ta *Table) AddColumn(name string, columnType string, collation string, extra string) {
    index := len(ta.Columns)
    ta.Columns = append(ta.Columns, TableColumn{Name: name, Collation: collation})
    ta.Columns[index].RawType = columnType

    if strings.Contains(columnType, "int") || strings.HasPrefix(columnType, "year") {
        ta.Columns[index].Type = TYPE_NUMBER
    } else if strings.HasPrefix(columnType, "float") ||
        strings.HasPrefix(columnType, "double") ||
        strings.HasPrefix(columnType, "decimal") {
        ta.Columns[index].Type = TYPE_FLOAT
    } else if strings.HasPrefix(columnType, "enum") {
        ta.Columns[index].Type = TYPE_ENUM
        ta.Columns[index].EnumValues = strings.Split(strings.Replace(
            strings.TrimSuffix(
                strings.TrimPrefix(
                    columnType, "enum("),
                ")"),
            "'", "", -1),
            ",")
    } else if strings.HasPrefix(columnType, "set") {
        ta.Columns[index].Type = TYPE_SET
        ta.Columns[index].SetValues = strings.Split(strings.Replace(
            strings.TrimSuffix(
                strings.TrimPrefix(
                    columnType, "set("),
                ")"),
            "'", "", -1),
            ",")
    } else if strings.HasPrefix(columnType, "datetime") {
        ta.Columns[index].Type = TYPE_DATETIME
    } else if strings.HasPrefix(columnType, "timestamp") {
        ta.Columns[index].Type = TYPE_TIMESTAMP
    } else if strings.HasPrefix(columnType, "time") {
        ta.Columns[index].Type = TYPE_TIME
    } else if "date" == columnType {
        ta.Columns[index].Type = TYPE_DATE
    } else if strings.HasPrefix(columnType, "bit") {
        ta.Columns[index].Type = TYPE_BIT
    } else if strings.HasPrefix(columnType, "json") {
        ta.Columns[index].Type = TYPE_JSON
    } else {
        ta.Columns[index].Type = TYPE_STRING
    }

    if strings.Contains(columnType, "unsigned") || strings.Contains(columnType, "zerofill") {
        ta.Columns[index].IsUnsigned = true
    }

    if extra == "auto_increment" {
        ta.Columns[index].IsAuto = true
    }
}

func (ta *Table) FindColumn(name string) int {
    for i, col := range ta.Columns {
        if col.Name == name {
            return i
        }
    }
    return -1
}

func (ta *Table) GetPKColumn(index int) *TableColumn {
    return &ta.Columns[ta.PKColumns[index]]
}

func (ta *Table) AddIndex(name string) (index *Index) {
    index = NewIndex(name)
    ta.Indexes = append(ta.Indexes, index)
    return index
}

// 获得表的 insert sql 语句模板
// 返回: insSQLTitTpl: Insert SQL Title 模板
//       insSQLValTpl: Insert SQL Value 模板
func (ta *Table) GetInsertSqlTemplate() (string, string) {

    if len(ta.insSQLTitTpl) == 0 || len(ta.insSQLValTpl) == 0 { // 如果没有定义insert的模板则从新生成一个
        columnNames := ""  // 字段名
        columnValues := "" // 字段值
        for _, column := range ta.Columns {
            columnNames += column.Name + ", "
            switch column.Type {
            case TYPE_NUMBER: // 数值型
                columnValues += `%d, `
            case TYPE_FLOAT: // double, float 浮点类型
                columnValues += `%f, `
            default: // 其他类型
                columnValues += `'%s', `
            }
        }

        // 去除 字符串的最后的 逗号和空格 `, `
        columnNames = columnNames[:len(columnNames)-2]
        columnValues = columnValues[:len(columnValues)-2]

        // 拼接出改表的insert SQL Title 模板
        insSQLTitTpl := fmt.Sprintf(
            `/* canal %s -> %s */ REPLACE INTO %s(%s) VALUES`,
            `%s.%s`,
            `%s.%s`,
            `%s`,
            columnNames)
        // 拼接出Insert SQL Value 模板
        insSQLValTpl := fmt.Sprintf(`(%s)`, columnValues)

        ta.insSQLTitTpl = insSQLTitTpl
        ta.insSQLValTpl = insSQLValTpl
    }

    return ta.insSQLTitTpl, ta.insSQLValTpl
}

// 获得表的 update sql 语句模板
// 返回: updSQLSetTpl: Update SQL Set 块 模板
//       updSQLWhereTpl: Update SQL Where 块 模板
func (ta *Table) GetUpdateSqlTemplate() (string, string) {
    if len(ta.updSQLSetTpl) == 0 || len(ta.updSQLWhereTpl) == 0 { // 如果没有定义update的模板则从新生成一个

        // 拼接Set模板
        updSQLSetTpl := `/* canal %s.%s -> %s.%s */UPDATE %s ` // 初始化 set 子句
        for _, column := range ta.Columns {
            switch column.Type {
            case TYPE_NUMBER: // 数值型
                updSQLSetTpl += column.Name + `=%d, `
            case TYPE_FLOAT: // double, float 浮点类型
                updSQLSetTpl += column.Name + `=%f, `
            default: // 其他类型
                updSQLSetTpl += column.Name + `='%s', `
            }
        }

        updSQLSetTpl = updSQLSetTpl[:len(updSQLSetTpl)-2] // 除去最后一个逗号(,)
        updSQLSetTpl += " "                               // 在最后添加一个空格

        // 拼凑 WHERE 模板
        updSQLWhereTpl := `WHERE ` // 初始化WHERE子句模板
        // 获得主键列
        for _, pkColIdx := range ta.PKColumns {
            column := ta.GetPKColumn(pkColIdx)
            switch column.Type {
            case TYPE_NUMBER: // 数值型
                updSQLWhereTpl += column.Name + `=%d, `
            case TYPE_FLOAT: // double, float 浮点类型
                updSQLWhereTpl += column.Name + `=%f, `
            default: // 其他类型
                updSQLWhereTpl += column.Name + `='%s', `
            }
        }
        updSQLWhereTpl = updSQLWhereTpl[:len(updSQLWhereTpl)-2] // 除去最后一个逗号(,)

        ta.updSQLSetTpl = updSQLSetTpl
        ta.updSQLWhereTpl = ta.updSQLWhereTpl

    }

    return ta.updSQLSetTpl, ta.updSQLWhereTpl
}

// 获得表的 delete sql 语句模板
func (ta *Table) GetDeleteSqlTemplate() string { return "" }

// 获得表列数
// 返回: 表的列个数
func (ta *Table) GetTableColNum() int {
    if ta.colNum == 0 {
        ta.colNum = len(ta.Columns)
    }

    return ta.colNum
}

func NewIndex(name string) *Index {
    return &Index{name, make([]string, 0, 8), make([]uint64, 0, 8)}
}

func (idx *Index) AddColumn(name string, cardinality uint64) {
    idx.Columns = append(idx.Columns, name)
    if cardinality == 0 {
        cardinality = uint64(len(idx.Cardinality) + 1)
    }
    idx.Cardinality = append(idx.Cardinality, cardinality)
}

func (idx *Index) FindColumn(name string) int {
    for i, colName := range idx.Columns {
        if name == colName {
            return i
        }
    }
    return -1
}

func IsTableExist(conn mysql.Executer, schema string, name string) (bool, error) {
    query := fmt.Sprintf("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' and TABLE_NAME = '%s' LIMIT 1", schema, name)
    r, err := conn.Execute(query)
    if err != nil {
        return false, errors.Trace(err)
    }

    return r.RowNumber() == 1, nil
}

func NewTableFromSqlDB(conn *sql.DB, schema string, name string) (*Table, error) {
    ta := &Table{
        Schema:  schema,
        Name:    name,
        Columns: make([]TableColumn, 0, 16),
        Indexes: make([]*Index, 0, 8),
    }

    if err := ta.fetchColumnsViaSqlDB(conn); err != nil {
        return nil, errors.Trace(err)
    }

    if err := ta.fetchIndexesViaSqlDB(conn); err != nil {
        return nil, errors.Trace(err)
    }

    return ta, nil
}

func NewTable(conn mysql.Executer, schema string, name string) (*Table, error) {
    ta := &Table{
        Schema:  schema,
        Name:    name,
        Columns: make([]TableColumn, 0, 16),
        Indexes: make([]*Index, 0, 8),
    }

    if err := ta.fetchColumns(conn); err != nil {
        return nil, errors.Trace(err)
    }

    if err := ta.fetchIndexes(conn); err != nil {
        return nil, errors.Trace(err)
    }

    return ta, nil
}

func (ta *Table) fetchColumns(conn mysql.Executer) error {
    r, err := conn.Execute(fmt.Sprintf("show full columns from `%s`.`%s`", ta.Schema, ta.Name))
    if err != nil {
        return errors.Trace(err)
    }

    for i := 0; i < r.RowNumber(); i++ {
        name, _ := r.GetString(i, 0)
        colType, _ := r.GetString(i, 1)
        collation, _ := r.GetString(i, 2)
        extra, _ := r.GetString(i, 6)

        ta.AddColumn(name, colType, collation, extra)
    }

    return nil
}

func (ta *Table) fetchColumnsViaSqlDB(conn *sql.DB) error {
    r, err := conn.Query(fmt.Sprintf("show full columns from `%s`.`%s`", ta.Schema, ta.Name))
    if err != nil {
        return errors.Trace(err)
    }

    defer r.Close()

    var unusedVal interface{}
    unused := &unusedVal

    for r.Next() {
        var name, colType, extra string
        var collation sql.NullString
        err := r.Scan(&name, &colType, &collation, &unused, &unused, &unused, &extra, &unused, &unused)
        if err != nil {
            return errors.Trace(err)
        }
        ta.AddColumn(name, colType, collation.String, extra)
    }

    return r.Err()
}

func (ta *Table) fetchIndexes(conn mysql.Executer) error {
    r, err := conn.Execute(fmt.Sprintf("show index from `%s`.`%s`", ta.Schema, ta.Name))
    if err != nil {
        return errors.Trace(err)
    }
    var currentIndex *Index
    currentName := ""

    for i := 0; i < r.RowNumber(); i++ {
        indexName, _ := r.GetString(i, 2)
        if currentName != indexName {
            currentIndex = ta.AddIndex(indexName)
            currentName = indexName
        }
        cardinality, _ := r.GetUint(i, 6)
        colName, _ := r.GetString(i, 4)
        currentIndex.AddColumn(colName, cardinality)
    }

    return ta.fetchPrimaryKeyColumns()

}

func (ta *Table) fetchIndexesViaSqlDB(conn *sql.DB) error {
    r, err := conn.Query(fmt.Sprintf("show index from `%s`.`%s`", ta.Schema, ta.Name))
    if err != nil {
        return errors.Trace(err)
    }

    defer r.Close()

    var currentIndex *Index
    currentName := ""

    var unusedVal interface{}
    unused := &unusedVal

    for r.Next() {
        var indexName, colName string
        var cardinality interface{}

        err := r.Scan(
            &unused,
            &unused,
            &indexName,
            &unused,
            &colName,
            &unused,
            &cardinality,
            &unused,
            &unused,
            &unused,
            &unused,
            &unused,
            &unused,
        )
        if err != nil {
            return errors.Trace(err)
        }

        if currentName != indexName {
            currentIndex = ta.AddIndex(indexName)
            currentName = indexName
        }

        c := toUint64(cardinality)
        currentIndex.AddColumn(colName, c)
    }

    return ta.fetchPrimaryKeyColumns()
}

func toUint64(i interface{}) uint64 {
    switch i := i.(type) {
    case int:
        return uint64(i)
    case int8:
        return uint64(i)
    case int16:
        return uint64(i)
    case int32:
        return uint64(i)
    case int64:
        return uint64(i)
    case uint:
        return uint64(i)
    case uint8:
        return uint64(i)
    case uint16:
        return uint64(i)
    case uint32:
        return uint64(i)
    case uint64:
        return uint64(i)
    }

    return 0
}

func (ta *Table) fetchPrimaryKeyColumns() error {
    if len(ta.Indexes) == 0 {
        return nil
    }

    pkIndex := ta.Indexes[0]
    if pkIndex.Name != "PRIMARY" {
        return nil
    }

    ta.PKColumns = make([]int, len(pkIndex.Columns))
    for i, pkCol := range pkIndex.Columns {
        ta.PKColumns[i] = ta.FindColumn(pkCol)
    }

    return nil
}
