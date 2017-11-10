package canal

import (
    "context"
    "fmt"
    "strconv"
    "strings"
    "sync"

    "github.com/daiguadaidai/go-mysql-migration/client"
    "github.com/daiguadaidai/go-mysql-migration/mysql"
    "github.com/daiguadaidai/go-mysql-migration/replication"
    "github.com/daiguadaidai/go-mysql-migration/schema"
    "github.com/juju/errors"
    log "github.com/sirupsen/logrus"
)

// Canal can sync your MySQL data into everywhere, like Elasticsearch, Redis, etc...
// MySQL must open row format for binlog
type Canal struct {
    m   sync.Mutex

    cfg *Config

    useGTID bool

    master *masterInfo
    syncer *replication.BinlogSyncer

    eventHandler EventHandler

    connLock sync.Mutex
    conn     *client.Conn

    wg  sync.WaitGroup

    tableLock sync.RWMutex
    tables    map[string]*schema.Table

    ctx    context.Context
    cancel context.CancelFunc
}

func NewCanal(cfg *Config) (*Canal, error) {
    c := new(Canal)
    c.cfg = cfg

    c.ctx, c.cancel = context.WithCancel(context.Background())

    c.eventHandler = &DummyEventHandler{}

    c.tables = make(map[string]*schema.Table)
    c.master = &masterInfo{}

    var err error

    if err = c.prepareSyncer(); err != nil {
        return nil, errors.Trace(err)
    }

    if err := c.checkBinlogRowFormat(); err != nil {
        return nil, errors.Trace(err)
    }

    return c, nil
}

// Start will first try to dump all data from MySQL master `mysqldump`,
// then sync from the binlog position in the dump data.
func (c *Canal) Start() error {
    // c.wg.Add(1)
    // go c.run()
    log.Info("canal Start func")
    c.run()

    return nil
}

// StartFrom will sync from the binlog position directly, ignore mysqldump.
func (c *Canal) StartFrom(pos mysql.Position) error {
    c.useGTID = false
    c.master.Update(pos)
    log.Info(pos.String())

    return c.Start()
}

func (c *Canal) StartFromGTID(set mysql.GTIDSet) error {
    c.useGTID = true
    c.master.UpdateGTID(set)

    return c.Start()
}

func (c *Canal) run() error {
    /* ----
       defer func() {
           c.wg.Done()
           c.cancel()
       }()
    */

    log.Info("----- cannal run ------")
    if err := c.runSyncBinlog(); err != nil {
        log.Errorf("canal start sync binlog err: %v", err)
        return errors.Trace(err)
    }

    return nil
}

func (c *Canal) Close() {
    log.Infof("closing canal")

    c.m.Lock()
    defer c.m.Unlock()

    c.cancel()
    c.connLock.Lock()
    c.conn.Close()
    c.conn = nil
    c.connLock.Unlock()
    c.syncer.Close()

    c.eventHandler.OnPosSynced(c.master.Position(), true)

    c.wg.Wait()
}

func (c *Canal) Ctx() context.Context {
    return c.ctx
}

func (c *Canal) GetTable(db string, table string) (*schema.Table, error) {
    key := fmt.Sprintf("%s.%s", db, table)
    c.tableLock.RLock()
    t, ok := c.tables[key]
    c.tableLock.RUnlock()

    if ok {
        return t, nil
    }

    t, err := schema.NewTable(c, db, table)
    if err != nil {
        // check table not exists
        if ok, err1 := schema.IsTableExist(c, db, table); err1 == nil && !ok {
            return nil, schema.ErrTableNotExist
        }

        return nil, errors.Trace(err)
    }

    c.tableLock.Lock()
    c.tables[key] = t
    c.tableLock.Unlock()

    return t, nil
}

// ClearTableCache clear table cache
func (c *Canal) ClearTableCache(db []byte, table []byte) {
    key := fmt.Sprintf("%s.%s", db, table)
    c.tableLock.Lock()
    delete(c.tables, key)
    c.tableLock.Unlock()
}

// Check MySQL binlog row image, must be in FULL, MINIMAL, NOBLOB
func (c *Canal) CheckBinlogRowImage(image string) error {
    // need to check MySQL binlog row image? full, minimal or noblob?
    // now only log
    if c.cfg.Flavor == mysql.MySQLFlavor {
        if res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_row_image"`); err != nil {
            return errors.Trace(err)
        } else {
            // MySQL has binlog row image from 5.6, so older will return empty
            rowImage, _ := res.GetString(0, 1)
            if rowImage != "" && !strings.EqualFold(rowImage, image) {
                return errors.Errorf("MySQL uses %s binlog row image, but we want %s", rowImage, image)
            }
        }
    }

    return nil
}

func (c *Canal) checkBinlogRowFormat() error {
    res, err := c.Execute(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
    if err != nil {
        return errors.Trace(err)
    } else if f, _ := res.GetString(0, 1); f != "ROW" {
        return errors.Errorf("binlog must ROW format, but %s now", f)
    }

    return nil
}

func (c *Canal) prepareSyncer() error {
    seps := strings.Split(c.cfg.Addr, ":")
    if len(seps) != 2 {
        return errors.Errorf("invalid mysql addr format %s, must host:port", c.cfg.Addr)
    }

    port, err := strconv.ParseUint(seps[1], 10, 16)
    if err != nil {
        return errors.Trace(err)
    }

    cfg := replication.BinlogSyncerConfig{
        ServerID:        c.cfg.ServerID,
        Flavor:          c.cfg.Flavor,
        Host:            seps[0],
        Port:            uint16(port),
        User:            c.cfg.User,
        Password:        c.cfg.Password,
        Charset:         c.cfg.Charset,
        HeartbeatPeriod: c.cfg.HeartbeatPeriod,
        ReadTimeout:     c.cfg.ReadTimeout,
    }

    c.syncer = replication.NewBinlogSyncer(cfg)

    return nil
}

// Execute a SQL
func (c *Canal) Execute(cmd string, args ...interface{}) (rr *mysql.Result, err error) {
    c.connLock.Lock()
    defer c.connLock.Unlock()

    retryNum := 3
    for i := 0; i < retryNum; i++ {
        if c.conn == nil {
            c.conn, err = client.Connect(c.cfg.Addr, c.cfg.User, c.cfg.Password, "")
            if err != nil {
                return nil, errors.Trace(err)
            }
        }

        rr, err = c.conn.Execute(cmd, args...)
        if err != nil && !mysql.ErrorEqual(err, mysql.ErrBadConn) {
            return
        } else if mysql.ErrorEqual(err, mysql.ErrBadConn) {
            c.conn.Close()
            c.conn = nil
            continue
        } else {
            return
        }
    }
    return
}

func (c *Canal) SyncedPosition() mysql.Position {
    return c.master.Position()
}
