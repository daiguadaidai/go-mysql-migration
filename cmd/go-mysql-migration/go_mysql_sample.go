package main

import (
    "context"
    "encoding/hex"
    "fmt"

    "github.com/daiguadaidai/go-mysql-migration/mysql"
    "github.com/daiguadaidai/go-mysql-migration/replication"
)

func main() {
    // Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
    // flavor is mysql or mariadb
    cfg := replication.BinlogSyncerConfig{
        ServerID: 100,
        Flavor:   "mysql",
        Host:     "10.10.10.12",
        Port:     3306,
        User:     "HH",
        Password: "oracle",
    }
    syncer := replication.NewBinlogSyncer(cfg)

    // Start sync with sepcified binlog file and position
    binlogFile := "mysql-bin.000010"
    binlogPos := (uint32)(0)
    streamer, _ := syncer.StartSync(mysql.Position{binlogFile, binlogPos})

    // or you can start a gtid replication like
    // streamer, _ := syncer.StartSyncGTID(gtidSet)
    // the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
    // the mariadb GTID set likes this "0-1-100"

    for {
        ev, _ := streamer.GetEvent(context.Background())
        // Dump event
        // fmt.Println(ev.Header.EventType.String())

        eventType := ev.Header.EventType.String()
        if eventType == "WriteRowsEventV2" {
            if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
                fmt.Println(eventType, rowsEvent.TableID, rowsEvent.Flags, rowsEvent.Table.Flags)
                fmt.Println("DB Name: ", string(rowsEvent.Table.Schema))
                fmt.Println("Table Name: ", string(rowsEvent.Table.Table))
                fmt.Println("ColumnType: ", hex.Dump(rowsEvent.Table.ColumnType))
                fmt.Println("ColumnColumnMeta: ", rowsEvent.Table.ColumnMeta)
                fmt.Println(rowsEvent.Rows)
            } else {
                fmt.Println("WriteRowsEventV2 isn't (*replication.RowsEvent) type")
            }
        } else if eventType == "UpdateRowsEventV2" {
            if rowsEvent, ok := ev.Event.(*replication.RowsEvent); ok {
                fmt.Println(eventType, rowsEvent.TableID, rowsEvent.Flags, rowsEvent.Table.Flags)
                fmt.Println("DB Name: ", string(rowsEvent.Table.Schema))
                fmt.Println("Table Name: ", string(rowsEvent.Table.Table))
                fmt.Println("ColumnType: ", hex.Dump(rowsEvent.Table.ColumnType))
                fmt.Println("ColumnColumnMeta: ", rowsEvent.Table.ColumnMeta)
                fmt.Println(rowsEvent.Rows)
            } else {
                fmt.Println("WriteRowsEventV2 isn't (*replication.RowsEvent) type")
            }
        }
        // ev.Dump(os.Stdout)
    }

    /*
       // or we can use a timeout context
       for {
           ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
           ev, err := streamer.GetEvent(ctx)
           cancel()

           if err == context.DeadlineExceeded {
               // meet timeout
               continue
           }

           ev.Dump(os.Stdout)
       }
    */
}
