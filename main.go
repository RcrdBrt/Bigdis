package main

import (
	"bigdis/config"
	"bigdis/server"
	"bigdis/storage"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coreos/go-systemd/v22/daemon"
)

func main() {
	configPath := flag.String("config", "", "path to config file")
	flag.Parse()

	// Init config
	config.Init(*configPath)

	storage.Init()

	// systemd-notify ready
	if _, err := daemon.SdNotify(false, daemon.SdNotifyReady); err != nil {
		panic(err)
	}
	go systemdNotify()

	// Handle SIGTERM
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigterm
		fmt.Println("\nStopping...")
		// Perform cleanup here
		daemon.SdNotify(false, daemon.SdNotifyStopping)
		os.Exit(0)
	}()

	// Start server
	if err := server.StartServer(); err != nil {
		panic(err)
	}
}

func systemdNotify() {
	for {
		daemon.SdNotify(false, daemon.SdNotifyWatchdog)
		time.Sleep(10 * time.Second)
	}
}
