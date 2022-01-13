package canal

import (
	"crypto/tls"
	"io/ioutil"
	"math/rand"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/pingcap/errors"
)

type DumpConfig struct {
	// mysqldump execution path, like mysqldump or /usr/bin/mysqldump, etc...
	// If not set, ignore using mysqldump.
	ExecutionPath string `toml:"mysqldump"`
	TableDB       string `toml:"table_db"`
	// Set to change the default protocol to connect with
	Protocol string `toml:"protocol"`
	// Dump only selected records. Quotes are mandatory
	Where string `toml:"where"`
	// Will override Databases, tables is in database table_db
	Tables []string `toml:"tables"`
	// Ignore table format is db.table
	IgnoreTables []string `toml:"ignore_tables"`
	Databases    []string `toml:"dbs"`
	// Set extra options
	ExtraOptions []string `toml:"extra_options"`
	// Set to change the default max_allowed_packet size
	MaxAllowedPacketMB int `toml:"max_allowed_packet_mb"`
	// If true, discard error msg, else, output to stderr
	DiscardErr bool `toml:"discard_err"`
	// Set true to skip --master-data if we have no privilege to do
	// 'FLUSH TABLES WITH READ LOCK'
	SkipMasterData bool `toml:"skip_master_data"`
}

type Config struct {
	// Set TLS config
	TLSConfig               *tls.Config
	TimestampStringLocation *time.Location
	User                    string `toml:"user"`
	Password                string `toml:"password"`
	Charset                 string `toml:"charset"`
	Flavor                  string `toml:"flavor"`
	Addr                    string `toml:"addr"`
	// IncludeTableRegex or ExcludeTableRegex should contain database name
	// Only a table which matches IncludeTableRegex and dismatches ExcludeTableRegex will be processed
	// eg, IncludeTableRegex : [".*\\.canal"], ExcludeTableRegex : ["mysql\\..*"]
	//     this will include all database's 'canal' table, except database 'mysql'
	// Default IncludeTableRegex and ExcludeTableRegex are empty, this will include all tables
	IncludeTableRegex []string      `toml:"include_table_regex"`
	ExcludeTableRegex []string      `toml:"exclude_table_regex"`
	Dump              DumpConfig    `toml:"dump"`
	ReadTimeout       time.Duration `toml:"read_timeout"`
	HeartbeatPeriod   time.Duration `toml:"heartbeat_period"`
	// maximum number of attempts to re-establish a broken connection, zero or negative number means infinite retry.
	// this configuration will not work if DisableRetrySync is true
	MaxReconnectAttempts int    `toml:"max_reconnect_attempts"`
	ServerID             uint32 `toml:"server_id"`
	ParseTime            bool   `toml:"parse_time"`
	// SemiSyncEnabled enables semi-sync or not.
	SemiSyncEnabled bool `toml:"semi_sync_enabled"`
	// discard row event without table meta
	DiscardNoMetaRowEvent bool `toml:"discard_no_meta_row_event"`
	// whether disable re-sync for broken connection
	DisableRetrySync bool `toml:"disable_retry_sync"`
	UseDecimal       bool `toml:"use_decimal"`
}

func NewConfigWithFile(name string) (*Config, error) {
	data, err := ioutil.ReadFile(name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return NewConfig(string(data))
}

func NewConfig(data string) (*Config, error) {
	var c Config

	_, err := toml.Decode(data, &c)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &c, nil
}

func NewDefaultConfig() *Config {
	c := new(Config)

	c.Addr = "127.0.0.1:3306"
	c.User = "root"
	c.Password = ""

	c.Charset = mysql.DEFAULT_CHARSET
	c.ServerID = uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001

	c.Flavor = "mysql"

	c.Dump.ExecutionPath = "mysqldump"
	c.Dump.DiscardErr = true
	c.Dump.SkipMasterData = false

	return c
}
