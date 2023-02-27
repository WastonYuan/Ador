package t_util

import (
	"encoding/json"
    "os"
    "fmt"
)

// participant

var Pconf *ConfigurationP

type ConfigurationP struct {
	Host string
	Server string
	Fallback bool
	Reordering bool
	Thread int
}

func InitConfigurationP() {
	Pconf = &ConfigurationP{}
}

func ReadJsonP(file_name string) {

	file, _ := os.Open(file_name)
	defer file.Close()
	decoder := json.NewDecoder(file)
	err := decoder.Decode(Pconf)
	if err != nil {
	  fmt.Println("error:", err)
	}
}


// coordinator

var Cconf *ConfigurationC

type ConfigurationC struct {
	Server []string
    Workload string
	Warehouse int
    NewOrderRate float64
    WriteRate float64
    Skew float64
	EpochSize int
}

func InitConfigurationC() {
	Cconf = &ConfigurationC{}
}

func ReadJsonC(file_name string) {

	file, _ := os.Open(file_name)
	defer file.Close()
	decoder := json.NewDecoder(file)
	err := decoder.Decode(Cconf)
	if err != nil {
	  fmt.Println("error:", err)
	}
}
