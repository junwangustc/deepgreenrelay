package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/eleme/deepgreenrelay/proxy"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	configPath string
	logFile    string
)

//Initialize flag option
func init() {
	flag.StringVar(&configPath, "config", "deepgreen.toml", "deepgreen relay config file path")
	flag.StringVar(&logFile, "logfile", "/tmp/deepgreenrelay.log", "log file")
	flag.Parse()
	runtime.GOMAXPROCS(runtime.NumCPU())
}

//Ccre
func initLog() {
	if logFile != "" {
		rotateOutput := &lumberjack.Logger{
			Filename:   logFile,
			MaxSize:    100,
			MaxBackups: 5,
			MaxAge:     7,
		}
		log.SetOutput(rotateOutput)
	}

}

func main() {
	initLog()
	config, err := proxy.LoadConfigFile(configPath)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	p, err := proxy.New(config)
	if err != nil {
		log.Fatal(err)
	}

	signal_chan := make(chan os.Signal, 1)
	signal.Notify(signal_chan, syscall.SIGUSR2)
	log.Println("start proxy...")
	p.Run()
}
