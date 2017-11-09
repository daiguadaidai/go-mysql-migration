package canal

import (
    "strconv"

    "github.com/daiguadaidai/go-mysql-migration/dump"
    "github.com/daiguadaidai/go-mysql-migration/schema"
    "github.com/juju/errors"
    log "github.com/sirupsen/logrus"
)

type dumpParseHandler struct {
    c    *Canal
    name string
    pos  uint64
}

func (h *dumpParseHandler) BinLog(name string, pos uint64) error {
    h.name = name
    h.pos = pos
    return nil
}

func (h *dumpParseHandler) Data(db string, table string, values []string) error {
    if err := h.c.ctx.Err(); err != nil {
        return err
    }

    tableInfo, err := h.c.GetTable(db, table)
    if err != nil {
        log.Errorf("get %s.%s information err: %v", db, table, err)
        return errors.Trace(err)
    }

    vs := make([]interface{}, len(values))

    for i, v := range values {
        if v == "NULL" {
            vs[i] = nil
        } else if v[0] != '\'' {
            if tableInfo.Columns[i].Type == schema.TYPE_NUMBER {
                n, err := strconv.ParseInt(v, 10, 64)
                if err != nil {
                    log.Errorf("parse row %v at %d error %v, skip", values, i, err)
                    return dump.ErrSkip
                }
                vs[i] = n
            } else if tableInfo.Columns[i].Type == schema.TYPE_FLOAT {
                f, err := strconv.ParseFloat(v, 64)
                if err != nil {
                    log.Errorf("parse row %v at %d error %v, skip", values, i, err)
                    return dump.ErrSkip
                }
                vs[i] = f
            } else {
                log.Errorf("parse row %v error, invalid type at %d, skip", values, i)
                return dump.ErrSkip
            }
        } else {
            vs[i] = v[1 : len(v)-1]
        }
    }

    events := newRowsEvent(tableInfo, InsertAction, [][]interface{}{vs})
    return h.c.eventHandler.OnRow(events)
}
