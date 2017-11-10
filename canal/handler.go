package canal

import (
    "bytes"
    "fmt"

    "github.com/daiguadaidai/go-mysql-migration/mysql"
    "github.com/daiguadaidai/go-mysql-migration/replication"
    "github.com/juju/errors"
    log "github.com/sirupsen/logrus"
)

type EventHandler interface {
    OnRotate(roateEvent *replication.RotateEvent) error
    OnDDL(nextPos mysql.Position, queryEvent *replication.QueryEvent) error
    OnRow(e *RowsEvent) error
    OnXID(nextPos mysql.Position) error
    OnGTID(gtid mysql.GTIDSet) error
    // OnPosSynced Use your own way to sync position. When force is true, sync position immediately.
    OnPosSynced(pos mysql.Position, force bool) error
    String() string
}

type DummyEventHandler struct {
}

func (this *DummyEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (this *DummyEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (this *DummyEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (this *DummyEventHandler) OnXID(mysql.Position) error             { return nil }
func (this *DummyEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (this *DummyEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (this *DummyEventHandler) String() string                         { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (this *Canal) SetEventHandler(h EventHandler) {
    this.eventHandler = h
}

// One db to One db migration Events Handler
type OTOEventHandler struct {
    InsBatchSize int
}

func (this *OTOEventHandler) OnRotate(*replication.RotateEvent) error             { return nil }
func (this *OTOEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error { return nil }

func (this *OTOEventHandler) OnRow(rowsEvent *RowsEvent) error {
    // binlog中解析的列数和当前表结构的中的列数不一致, 返回错误
    if rowsEvent.Table.GetTableColNum() != len(rowsEvent.Rows[0]) {
        errInfo := fmt.Sprintf(
            "Table: %s.%s Now column num[%d] diffrent binlog row data column num[%d] ",
            rowsEvent.Table.Schema,
            rowsEvent.Table.Name,
            rowsEvent.Table.GetTableColNum(),
            len(rowsEvent.Rows[0]))

        return errors.New(errInfo)
    }

    log.Infof(
        "OTOEventHandler OnRow. Table: %v, Action:, %v, rows %v, Pri Key: %v %v %v",
        rowsEvent.Table.Name,
        rowsEvent.Action,
        rowsEvent.Rows,
        rowsEvent.Table.PKColumns,
        rowsEvent.Table.GetPKColumn(0).Name,
        rowsEvent.Table.Columns)

    switch rowsEvent.Action {
    case InsertAction: // 处理Insert事件
        return this.insertEventHandler(rowsEvent)
    case UpdateAction: // 处理Insert事件
        return this.updateEventHandler(rowsEvent)
    case DeleteAction: // 处理Insert事件
        fmt.Println("delete")
    }

    return nil
}

func (this *OTOEventHandler) OnXID(mysql.Position) error             { return nil }
func (this *OTOEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (this *OTOEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (this *OTOEventHandler) String() string                         { return "OTOEventHandler" }

// 处理Insert 事件
func (this *OTOEventHandler) insertEventHandler(rowsEvent *RowsEvent) error {
    // 获取表的当前列个数
    // 获取Insert SQL模板
    insSqlTitTpl, insSqlValTpl := rowsEvent.Table.GetInsertSqlTemplate()

    // 将带有占位符的SQL Title 变成实际的Insert Title
    insSQLTit := fmt.Sprintf(insSqlTitTpl,
        rowsEvent.Table.Schema,
        rowsEvent.Table.Name,
        rowsEvent.Table.Schema,
        rowsEvent.Table.Name,
        rowsEvent.Table.Name)

    var insSQL bytes.Buffer        // 定义拼凑字符串buffer
    insSQL.WriteString(insSQLTit)  // 添加Insert SQL Title
    rowsNum := len(rowsEvent.Rows) // 获取insert行数
    for index, row := range rowsEvent.Rows {
        insSQLVal := fmt.Sprintf(insSqlValTpl, row...)
        insSQL.WriteString(insSQLVal)

        if rowsNum == index+1 {
            insSQL.WriteString(";")
            this.executeInsertSQL(insSQL.String()) // 执行sql

        } else if (index+1)%this.InsBatchSize == 0 {
            insSQL.WriteString(";")
            this.executeInsertSQL(insSQL.String()) // 执行sql
            // 重新下一个批量Insert
            insSQL.Reset()                // 情空buffer中的内容
            insSQL.WriteString(insSQLTit) // 添加Insert SQL Title
        } else {
            insSQL.WriteString(",")
        }
    }

    return nil
}

// 处理 update 事件
func (this *OTOEventHandler) updateEventHandler(rowsEvent *RowsEvent) error {
    // 获取表的当前列个数
    // 获取Insert SQL模板
    updSQLSetTpl, updSQLWhereTpl := rowsEvent.Table.GetUpdateSqlTemplate()
    fmt.Println(updSQLSetTpl, updSQLWhereTpl)

    // 定义Update语句的前后行
    var oldRow []interface{}
    var newRow []interface{}
    for index, row := range rowsEvent.Rows {
        if (index+1)%2 == 0 { // 更新后的值 newRow
            newRow = row
        } else { // 更新前的值 oldRow
            oldRow = row
        }

        // 拿到了前后行的值可以进行对UPDATE 语句处理
        if (index+1)%2 == 0 {
            // 判断主键值是否发生变化, 发生变化生成 DELETE, 和REPLACE INTO 语句
            if rowsEvent.IsPKValueCons(oldRow, newRow) == true { // UPDATE
                fmt.Println(oldRow)
                fmt.Println(newRow)
            } else { // DELETE, REPLACE INTO
            }
        }
    }

    return nil
}

// 执行 Insert SQL
func (this *OTOEventHandler) executeInsertSQL(insSql string) error {
    fmt.Println(insSql)
    return nil
}

// One db to Shard DB migration Evnet Handler
type OTSEventHandler struct{}

func (this *OTSEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (this *OTSEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (this *OTSEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (this *OTSEventHandler) OnXID(mysql.Position) error             { return nil }
func (this *OTSEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (this *OTSEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (this *OTSEventHandler) String() string                         { return "OTSEventHandler" }

// Shard DB to Shard DB, but the same of pieces.
type STSEventHandler struct{}

func (this *STSEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (this *STSEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (this *STSEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (this *STSEventHandler) OnXID(mysql.Position) error             { return nil }
func (this *STSEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (this *STSEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (this *STSEventHandler) String() string                         { return "STSEventHandler" }

// Shard DB to More Shard DB, change pieces.
type STMSEventHandler struct{}

func (this *STMSEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (this *STMSEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (this *STMSEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (this *STMSEventHandler) OnXID(mysql.Position) error             { return nil }
func (this *STMSEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (this *STMSEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (this *STMSEventHandler) String() string                         { return "STMSEventHandler" }
