package config

import (
	_ "embed"
	"encoding/json"
	"os"
)

//go:embed default.json
var defaultConfig []byte

var Config struct {
	Server struct {
		Host            string `json:"host"`
		Port            int    `json:"port"`
		SystemdWatchdog bool   `json:"systemd_watchdog"`
	} `json:"server"`
	Storage struct {
		Path        string `json:"path"`
		JournalMode string `json:"journal_mode"`
		Synchronous string `json:"synchronous"`
		GCInterval  int    `json:"gc_interval"`
	} `json:"storage"`
}

var (
	journalModes     = map[string]struct{}{}
	synchronousModes = map[string]struct{}{}
)

func Init(configPath string) {
	var content []byte
	var err error
	// if configPath is not valid, use defaultConfig
	if configPath == "" {
		content = defaultConfig
	} else {
		content, err = os.ReadFile(configPath)
		if err != nil {
			panic(err)
		}
	}

	err = json.Unmarshal(content, &Config)
	if err != nil {
		panic(err)
	}

	// sanitize content
	if Config.Server.Host == "" {
		Config.Server.Host = "localhost"
	}
	if Config.Server.Port == 0 {
		Config.Server.Port = 6389
	}

	if Config.Storage.Path == "" {
		Config.Storage.Path = "bigdis.db"
	}

	journalModes["wal"] = struct{}{}
	journalModes["delete"] = struct{}{}
	journalModes["truncate"] = struct{}{}
	journalModes["persist"] = struct{}{}
	journalModes["memory"] = struct{}{}
	journalModes["off"] = struct{}{}

	if _, ok := journalModes[Config.Storage.JournalMode]; !ok {
		Config.Storage.JournalMode = "wal"
	}

	synchronousModes["off"] = struct{}{}
	synchronousModes["normal"] = struct{}{}
	synchronousModes["full"] = struct{}{}
	synchronousModes["extra"] = struct{}{}

	if _, ok := synchronousModes[Config.Storage.Synchronous]; !ok {
		Config.Storage.Synchronous = "normal"
	}
}
