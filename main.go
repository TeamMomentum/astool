package main

import (
	"context"
	"flag"
	"os"

	"github.com/TeamMomentum/astool/del"

	"github.com/google/subcommands"
)

var (
	version = "v0.0.0"
	date    string
)

func init() {
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	subcommands.Register(del.Cmd("del"), "")
}

func main() {
	var (
		showVersion = flag.Bool("version", false, "show version")
	)

	flag.Parse()
	if *showVersion {
		printVersion()
		os.Exit(int(subcommands.ExitSuccess))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	os.Exit(int(subcommands.Execute(ctx)))
}

func printVersion() {
	println("astool", version, date)
}
