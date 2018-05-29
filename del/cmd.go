package del

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/TeamMomentum/bs-url-normalizer/lib/urls"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/google/subcommands"
)

type cmd struct {
	set  *string
	file *string
	host *string
	port *int

	name   string
	Output io.Writer
	Error  io.Writer
}

// Cmd creates new command
func Cmd(name string) subcommands.Command {
	if len(name) == 0 {
		panic("please set command name")
	}
	return &cmd{
		name:   name,
		Output: os.Stdout,
		Error:  os.Stderr,
	}
}

// Name implements subcommands.Commander interface
func (c *cmd) Name() string {
	return c.name
}

// Synopsis implements subcommands.Commander interface
func (c *cmd) Synopsis() string {
	return "delete Aerospike record"
}

// Usage implements subcommands.Commander interface
func (c *cmd) Usage() string {
	return `Usage: fdel -set SET [-file <file>] [KEY]`
}

// SetFlags implements subcommands.Commander interface
func (c *cmd) SetFlags(f *flag.FlagSet) {
	c.set = f.String("set", "swan.page", "Aerospike NAMESPACE.SET")
	c.file = f.String("file", "", "read key for deleting from file")
	c.host = f.String("host", "localhost", "Aerospike hostname")
	c.port = f.Int("port", 3000, "Aerospike port number")
}

// Execute implements subcommands.Commander interface
func (c *cmd) Execute(_ context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	args := f.Args()

	if len(*c.set) == 0 || len(*c.host) == 0 || *c.port == 0 {
		return subcommands.ExitUsageError
	}

	client, err := as.NewClient(*c.host, *c.port)
	if err != nil {
		println(err.Error())
		return subcommands.ExitFailure
	}

	switch {
	default:
		return subcommands.ExitUsageError
	case len(*c.file) > 0:
		if err := deleteRecords(client, *c.set, *c.file); err != nil {
			println(err.Error())
			return subcommands.ExitFailure
		}
	case len(args) > 0:
		if err := deleteRecord(client, *c.set, args[0]); err != nil {
			println(err.Error())
			return subcommands.ExitFailure
		}
	}

	return subcommands.ExitSuccess
}

func deleteRecord(c *as.Client, set, uri string) error {
	ns, sn, err := splitNamespaceSet(set)
	if err != nil {
		return err
	}
	return _deleteRecord(c, ns, sn, uri)
}

func deleteRecords(c *as.Client, set, file string) error {
	ns, sn, err := splitNamespaceSet(set)
	if err != nil {
		return err
	}

	f, err := os.Open(file)
	if err != nil {
		return err
	}

	numErr := 0
	s := bufio.NewScanner(f)
	for s.Scan() {
		uri := s.Text()
		if err := _deleteRecord(c, ns, sn, uri); err != nil {
			numErr++
			println(err.Error(), "url="+uri)
			continue
		}
	}

	if numErr > 0 {
		return fmt.Errorf("%d errors occure", numErr)
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

func _deleteRecord(c *as.Client, ns, sn, uri string) error {
	u, err := url.Parse(uri)
	if err != nil {
		return err
	}

	uri = urls.FirstNormalizeURL(u)
	key, err := as.NewKey(ns, sn, uri)
	if err != nil {
		return err
	}

	_, err := c.Delete(nil, key)

	return err
}
