package main

import (
    "fmt"
    "os"

    "github.com/daiguadaidai/go-mysql-migration/canal"
    "github.com/daiguadaidai/go-mysql-migration/mysql"
    log "github.com/sirupsen/logrus"
)

func init() {
    // Log as JSON instead of the default ASCII formatter.
    // 设置日志输出格式
    // log.SetFormatter(&log.JSONFormatter{}) // Json 格式
    logFormatter := new(log.TextFormatter) // 文本格式
    logFormatter.DisableColors = true
    logFormatter.FullTimestamp = true
    logFormatter.TimestampFormat = "2006-01-02 15:04:05.000000"
    log.SetFormatter(logFormatter)

    // Output to stdout instead of the default stderr
    // Can be any io.Writer, see below for File example
    log.SetOutput(os.Stdout)

    // Only log the warning severity or above.
    log.SetLevel(log.InfoLevel)
}

func main() {
    cfg := canal.NewDefaultConfig()
    cfg.Addr = "10.10.10.12:3306"
    cfg.User = "HH"
    cfg.Password = "oracle"
    fmt.Println(cfg)

    channel, err := canal.NewCanal(cfg)
    if err != nil {
        log.Error(err)
    }
    eventHandler := new(canal.OTOEventHandler)
    channel.SetEventHandler(eventHandler)

    channel.StartFrom(mysql.Position{"mysql-bin.000010", 0})
}
