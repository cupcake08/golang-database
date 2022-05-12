package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)

type User struct {
	Name    string
	Age     json.Number
	Contact string
	Address Address
	Company string
}

type Address struct {
	City    string
	State   string
	Country string
	Pincode json.Number
}

const Version = "1.0.0"

type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}

	Driver struct {
		mu      sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	}
)

type Options struct {
	Logger Logger
}

func New(dir string, options *Options) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := Options{}
	if options != nil {
		opts = *options
	}
	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)
	}
	driver := Driver{
		dir:     dir,
		mutexes: make(map[string]*sync.Mutex),
		log:     opts.Logger,
	}
	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("Using '%s' (database already exist)\n", dir)
		return &driver, nil
	}
	opts.Logger.Debug("Creating database '%s'...\n", dir)
	return &driver, os.MkdirAll(dir, 0755)
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return fi, err
}

func (d *Driver) Write(collection, resources string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	if resources == "" {
		return fmt.Errorf("missing resource - unable to save record (no name)")
	}
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	dir := filepath.Join(d.dir, collection)
	fnlPath := filepath.Join(dir, resources+".json")
	tmpPath := fnlPath + ".tmp"
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}
	b = append(b, byte('\n'))
	if err := ioutil.WriteFile(tmpPath, b, 0644); err != nil {
		return err
	}

	return os.Rename(tmpPath, fnlPath)
}

func (d *Driver) ReadAll(collection string) ([]string, error) {
	if collection == "" {
		return nil, fmt.Errorf("collection name cannot be empty")
	}
	dir := filepath.Join(d.dir, collection)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var records []string

	for _, file := range files {
		data, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}
		records = append(records, string(data))
	}

	return records, nil
}

func (d *Driver) Delete(collection, resource string) error {
	path := filepath.Join(collection, resource)
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	dir := filepath.Join(d.dir, path)

	switch fi, err := stat(dir); {
	case fi == nil, err != nil:
		return fmt.Errorf("unable to find file or dir named %v", path)
	case fi.Mode().IsDir():
		return os.RemoveAll(dir)
	case fi.Mode().IsRegular():
		return os.RemoveAll(dir + ".json")
	}

	return nil
}

func (d *Driver) Read(collection, resource string, v interface{}) error {
	if collection == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	if resource == "" {
		return fmt.Errorf("cissing resource - unable to read record (no name)")
	}

	record := filepath.Join(d.dir, collection, resource+".json")
	if _, err := stat(record); err != nil {
		return err
	}

	b, err := ioutil.ReadFile(record + ".json")
	if err != nil {
		return err
	}
	return json.Unmarshal(b, &v)
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mu.Lock()
	defer d.mu.Unlock()
	m, ok := d.mutexes[collection]
	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

func main() {
	dir := "./"

	db, err := New(dir, nil)
	if err != nil {
		log.Fatal(err)
	}

	employees := []User{
		{"John", "30", "9079897225", Address{"Bangalore", "Karnataka", "India", "560037"}, "Google"},
		{"Mary", "25", "2379492701", Address{"Hydrabad", "Telangana", "India", "560037"}, "Meta"},
		{"Peter", "35", "9079897225", Address{"Bangalore", "Karnataka", "India", "560037"}, "Google"},
	}

	for _, val := range employees {
		db.Write("users", val.Name, User{val.Name, val.Age, val.Contact, val.Address, val.Company})
	}

	record, err := db.ReadAll("users")
	if err != nil {
		log.Fatal(err)
	}

	var allUsers []User

	for _, f := range record {
		employeeFound := User{}
		if err := json.Unmarshal([]byte(f), &employeeFound); err != nil {
			log.Fatal(err)
		}
		allUsers = append(allUsers, employeeFound)
	}
	fmt.Println(allUsers)

	// if err = db.Delete("users","John"); err != nil {
	// 	log.Fatal(err)
	// }

	if err = db.Delete("users", ""); err != nil {
		log.Fatal(err)
	}
}
