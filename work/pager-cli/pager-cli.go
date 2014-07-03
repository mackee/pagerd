package main

import (
	"github.com/codegangsta/cli"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "pager-cli"
	app.Version = Version
	app.Usage = ""
	app.Author = "mackee"
	app.Email = "macopy123@gmail.com"
	app.Commands = Commands

	app.Run(os.Args)
}
