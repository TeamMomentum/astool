package get

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/subcommands"
)

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
	file *string
	host *string
	port *int

	name    string
	output  io.Writer
	logger  *log.Logger
	jsonenc *json.Encoder
}

func (c *command) outf(format string, v ...interface{}) {
	fmt.Fprintf(c.output, format, v...)
}

func (c *command) logf(format string, v ...interface{}) {
	c.logger.Printf(format, v...)
}

// Cmd initialize get command
func Cmd(name string, options ...Option) subcommands.Command {
	if len(name) == 0 {
		panic("please set command name")
	}

	cmd = &command{
		name:   name,
		output: os.Stdout,
		logger: log.New(os.Stderr, "", 0),
	}

	cmd.jsonenc = json.NewEncoder(cmd.output)

	for _, opt := range options {
		opt.apply(cmd)
	}

	return cmd
}

// Name implements subcommands.Commander interface
func (c *command) Name() string {
	return c.name
}

// Synopsis implements subcommands.Commander interface
func (c *command) Synopsis() string {
	return "get Aerospike record"
}

// Usage implements subcommands.Commander interface
func (c *command) Usage() string {
	return "Usage: get -set NAMESPACE.SET [-file FILE | KEY1,KEY2,...]\n"
}

// SetFlags implements subcommands.Commander interface
func (c *command) SetFlags(f *flag.FlagSet) {
	c.set = f.String("set", "", "Aerospike NAMESPACE.SET")
	c.file = f.String("file", "", "read keys of Records from file")
	c.host = f.String("host", "localhost", "Aerospike hostname")
	c.port = f.Int("port", 3000, "Aerospike port number")
}

// Execute implements subcommands.Commander interface
func (c *command) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	args := f.Args()

	if len(*c.set) == 0 || len(*c.host) == 0 || *c.port == 0 {
		f.Usage()
		return subcommands.ExitUsageError
	}

	client, err := as.NewClient(*c.host, *c.port)
	if err != nil {
		c.logf("could not get aerospike client: %s", err)
		return subcommands.ExitFailure
	}

	switch {
	default:
		f.Usage()
		return subcommands.ExitUsageError
	case len(*c.file) > 0:
		if err := getRecordsFromFile(client, *c.set, *c.file); err != nil {
			c.logf("could not get aerospike records: %s", err)
			return subcommands.ExitFailure
		}
	case len(args) > 0:
		if err := getRecords(client, *c.set, args...); err != nil {
			c.logf("could not get aerospike records: %s", err)
			return subcommands.ExitFailure
		}
	}

	return subcommands.ExitSuccess
}

func getRecords(c *as.Client, set string, keys ...string) error {
	ns, sn, err := splitNamespaceSet(set)
	if err != nil {
		return err
	}

	var (
		sc = 0
		fc = 0
	)

	for _, key := range keys {
		rec, err := getRecord(c, ns, sn, key)
		if err != nil {
			fc++
			cmd.logf("fail to get %s: %s", key, err)
			continue
		}

		sc++
		printRecord(key, rec)
	}

	cmd.logf("success=%d failure=%d", sc, fc)

	if fc > 0 {
		return fmt.Errorf("there are %d errors", fc)
	}

	return nil
}

func getRecordsFromFile(c *as.Client, set, file string) error {
	ns, sn, err := splitNamespaceSet(set)
	if err != nil {
		return err
	}

	f, err := os.Open(file)
	if err != nil {
		return err
	}

	var (
		sc = 0
		fc = 0
		s  = bufio.NewScanner(f)
	)

	for s.Scan() {
		key := s.Text()

		rec, err := getRecord(c, ns, sn, key)
		if err != nil {
			fc++
			cmd.logf("fail to get %s: %s", key, err)
			continue
		}

		sc++
		printRecord(key, rec)
	}

	cmd.logf("success=%d failure=%d", sc, fc)

	if fc > 0 {
		return fmt.Errorf("there are %d errors", fc)
	}

	return s.Err()
}

func splitNamespaceSet(src string) (string, string, error) {
	p := strings.Split(src, ".")
	if len(p) != 2 {
		return "", "", fmt.Errorf("invalid set name: %s", src)
	}

	return p[0], p[1], nil
}

func getRecord(c *as.Client, ns, sn, ksrc string) (*as.Record, error) {
	key, err := as.NewKey(ns, sn, ksrc)
	if err != nil {
		return nil, err
	}

	return c.Get(nil, key)
}

func printRecord(key string, rec *as.Record) {
	if cmd == nil {
		log.Fatal("get cmd not initialized")
	}

	bins := map[string]interface{}{}

	for k, v := range rec.Bins {
		bins[k] = toJSON(v)
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
