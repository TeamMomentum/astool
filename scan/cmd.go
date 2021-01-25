package scan

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/subcommands"
)

// cmd is used for singleton instance
var cmd *command

type Option interface {
	apply(*command)
}

type option struct {
	f func(*command)
}

func (o *option) apply(c *command) {
	o.f(c)
}

func WithLogger(logger *log.Logger) Option {
	return &option{
		f: func(c *command) {
			c.logger = logger
		},
	}
}

type output struct {
	Key  string                 `json:"key"`
	TTL  uint32                 `json:"ttl"`
	Gen  uint32                 `json:"gen"`
	Bins map[string]interface{} `json:"bins"`
}

type command struct {
	set  *string
	host *string
	port *int

	logger  *log.Logger
	jsonenc *json.Encoder
}

func (c *command) logf(format string, v ...interface{}) {
	c.logger.Printf(format, v...)
}

// Cmd initialize get command
func Cmd(options ...Option) subcommands.Command {
	cmd = &command{
		logger:  log.New(os.Stderr, "", 0),
		jsonenc: json.NewEncoder(os.Stdout),
	}

	for _, opt := range options {
		opt.apply(cmd)
	}

	return cmd
}

// Name implements subcommands.Commander interface
func (c *command) Name() string {
	return "scan"
}

// Synopsis implements subcommands.Commander interface
func (c *command) Synopsis() string {
	return "scan Aerospike record"
}

// Usage implements subcommands.Commander interface
func (c *command) Usage() string {
	return "Usage: scan -set NAMESPACE.SET\n"
}

// SetFlags implements subcommands.Commander interface
func (c *command) SetFlags(f *flag.FlagSet) {
	c.set = f.String("set", "", "Aerospike NAMESPACE.SET")
	c.host = f.String("host", "localhost", "Aerospike hostname")
	c.port = f.Int("port", 3000, "Aerospike port number")
}

// Execute implements subcommands.Commander interface
func (c *command) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if len(*c.set) == 0 || len(*c.host) == 0 || *c.port == 0 {
		f.Usage()
		return subcommands.ExitUsageError
	}

	client, err := as.NewClient(*c.host, *c.port)
	if err != nil {
		c.logf("could not get aerospike client: %s", err)
		return subcommands.ExitFailure
	}

	if err := scanRecords(client, *c.set); err != nil {
		c.logf("could not get aerospike records: %s", err)
		return subcommands.ExitFailure
	}

	return subcommands.ExitSuccess
}

func scanRecords(c *as.Client, set string) error {
	ns, sn, err := splitNamespaceSet(set)
	if err != nil {
		return err
	}

	p := as.NewScanPolicy()
	p.Priority = as.LOW
	p.MaxConcurrentNodes = 1

	scanner, err := c.ScanAll(p, ns, sn)
	if err != nil {
		return err
	}

	var (
		sc = 0
		fc = 0
	)

	for res := range scanner.Results() {
		if res.Err != nil {
			fc++
			cmd.logf("fail to scan: %s", res.Err)
			continue
		}

		sc++
		printRecord(res.Record)
	}

	cmd.logf("success=%d failure=%d", sc, fc)

	if fc > 0 {
		return fmt.Errorf("there are %d errors", fc)
	}

	return nil
}

func splitNamespaceSet(src string) (string, string, error) {
	p := strings.Split(src, ".")
	if len(p) != 2 {
		return "", "", fmt.Errorf("invalid set name: %s", src)
	}

	return p[0], p[1], nil
}

func printRecord(rec *as.Record) {
	if cmd == nil {
		log.Fatal("get cmd not initialized")
	}

	bins := map[string]interface{}{}

	for k, v := range rec.Bins {
		bins[k] = toJSON(v)
	}

	key := ""

	if v := rec.Key.Value(); v != nil {
		key = rec.Key.Value().String()
	} else {
		key = "digest:" + base64.StdEncoding.EncodeToString(rec.Key.Digest())
	}

	data := output{
		Key:  key,
		TTL:  rec.Expiration,
		Gen:  rec.Generation,
		Bins: bins,
	}

	if err := cmd.jsonenc.Encode(&data); err != nil {
		cmd.logf("could not print %s: %s", key, err)
	}
}

func toJSON(v interface{}) interface{} {
	vv, ok := v.(map[interface{}]interface{})
	if !ok {
		return v
	}

	m := map[string]interface{}{}

	for k, vvv := range vv {
		m[fmt.Sprintf("%s", k)] = toJSON(vvv)
	}

	return m
}
