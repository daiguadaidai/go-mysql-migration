package canal

import (
    "fmt"

    "github.com/daiguadaidai/go-mysql-migration/mysql"
    "github.com/daiguadaidai/go-mysql-migration/replication"
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

func (h *DummyEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (h *DummyEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (h *DummyEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (h *DummyEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *DummyEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *DummyEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *DummyEventHandler) String() string                         { return "DummyEventHandler" }

// `SetEventHandler` registers the sync handler, you must register your
// own handler before starting Canal.
func (c *Canal) SetEventHandler(h EventHandler) {
    c.eventHandler = h
}

// One db to One db migration Events Handler
type OTOEventHandler struct{}

func (h *OTOEventHandler) OnRotate(*replication.RotateEvent) error             { return nil }
func (h *OTOEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error { return nil }

func (h *OTOEventHandler) OnRow(rowsEvent *RowsEvent) error {
    log.Infof(
        "OTOEventHandler OnRow. Table: %v, Action:, %v, rows %v, Pri Key: %v %v %v",
        rowsEvent.Table.Name,
        rowsEvent.Action,
        rowsEvent.Rows,
        rowsEvent.Table.PKColumns,
        rowsEvent.Table.GetPKColumn(0).Name,
        rowsEvent.Table.Columns,
    )
    insertSql := rowsEvent.Table.GetInsertSqlTemplate()
    fmt.Println(insertSql)

    return nil
}

func (h *OTOEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *OTOEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *OTOEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *OTOEventHandler) String() string                         { return "OTOEventHandler" }

// One db to Shard DB migration Evnet Handler
type OTSEventHandler struct{}

func (h *OTSEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (h *OTSEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (h *OTSEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (h *OTSEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *OTSEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *OTSEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *OTSEventHandler) String() string                         { return "OTSEventHandler" }

// Shard DB to Shard DB, but the same of pieces.
type STSEventHandler struct{}

func (h *STSEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (h *STSEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (h *STSEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (h *STSEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *STSEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *STSEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *STSEventHandler) String() string                         { return "STSEventHandler" }

// Shard DB to More Shard DB, change pieces.
type STMSEventHandler struct{}

func (h *STMSEventHandler) OnRotate(*replication.RotateEvent) error { return nil }
func (h *STMSEventHandler) OnDDL(mysql.Position, *replication.QueryEvent) error {
    return nil
}
func (h *STMSEventHandler) OnRow(*RowsEvent) error                 { return nil }
func (h *STMSEventHandler) OnXID(mysql.Position) error             { return nil }
func (h *STMSEventHandler) OnGTID(mysql.GTIDSet) error             { return nil }
func (h *STMSEventHandler) OnPosSynced(mysql.Position, bool) error { return nil }
func (h *STMSEventHandler) String() string                         { return "STMSEventHandler" }
