package main

import (
	"bufio"
	"fmt"
	"go-zookeeper/zk"
	"os"
	"strconv"
	"strings"
	"time"
)

// print zkevent information
func zkEventWatcher(event <-chan zk.Event) {
	for {
		e := <-event
		fmt.Println(e)
	}
}

func showHelpInfo() {
	helpInfo :=
		`ZooKeeper -server host:port cmd args
	stat path [watch]
	set path data [version]
	ls path [watch]
	delquota [-n|-b] path
	ls2 path [watch]
	setAcl path acl
	setquota -n|-b val path
	history
	redo cmdno
	printwatches on|off
	delete path [version]
	sync path
	listquota path
	rmr path
	get path [watch]
	create [-s] [-e] path data acl
	addauth scheme auth
	quit
	getAcl path
	close
	connect host:port`
	fmt.Println(helpInfo)
}

func showNodeStatus(status *zk.Stat) {
	fmt.Printf("dataLength = %d\n", status.DataLength)
	fmt.Printf("cZxid = 0x%02x\n", status.Czxid)
	fmt.Printf("ctime = %s\n", time.Unix(0, status.Ctime*int64(time.Millisecond)).String())
	fmt.Printf("mZxid = 0x%02x\n", status.Mzxid)
	fmt.Printf("mtime = %s\n", time.Unix(0, status.Mtime*int64(time.Millisecond)).String())
	fmt.Printf("pZxid = 0x%02x\n", status.Pzxid)
	fmt.Printf("cversion = 0x%02x\n", status.Cversion)
	fmt.Printf("dataversion = 0x%02x\n", status.Version)
	fmt.Printf("aclVersion = 0x%02x\n", status.Aversion)
	fmt.Printf("ephemeralOwner = 0x%02x\n", status.EphemeralOwner)
	fmt.Printf("numChildren = %d\n", status.NumChildren)
}

// handle command
func commandHandler(conn *zk.Conn, command string) {
	fmt.Println("command: ", command)
	args := strings.Split(command, " ")
	switch args[0] {
	case "create":
		cmdCreate(conn, args[1:])
	case "delete":
		cmdDelete(conn, args[1:])
	case "ls":
		cmdLs(conn, args[1:])
	case "set":
		cmdSet(conn, args[1:])
	case "get":
		cmdGet(conn, args[1:])
	case "getAcl":
		cmdGetAcl(conn, args[1:])
	case "setAcl":
		cmdSetAcl(conn, args[1:])
	default:
		showHelpInfo()
	}
}

func cmdCreate(conn *zk.Conn, args []string) {
	if len(args) < 2 {
		fmt.Println("arg invalid: ", args)
		return
	}
	var nodeFlag int32
	switch args[0] {
	case "-s":
		nodeFlag = zk.FlagSequence
		args = args[1:]
	case "-e":
		nodeFlag = zk.FlagEphemeral
		args = args[1:]
	default:
		nodeFlag = zk.FlagEphemeral
	}

	if len(args) < 2 {
		fmt.Println("arg invalid: ", args)
		return
	}

	var acl []zk.ACL
	if len(args) == 3 {
		// todo: generate acl
		acl = zk.WorldACL(zk.PermAll)
	} else { // default acl
		acl = zk.WorldACL(zk.PermAll)
	}
	if ret, err := conn.Create(args[0], []byte(args[1]), nodeFlag, acl); err != nil {
		fmt.Printf("create node error:", args, err)
	} else {
		fmt.Println("created", ret)
	}
}

func cmdDelete(conn *zk.Conn, args []string) {
	if len(args) < 1 {
		fmt.Println("arg invalid: ", args)
		return
	}

	var version int32
	if len(args) == 2 {
		value, _ := strconv.ParseInt(args[1], 10, 32)
		version = int32(value)
	} else { // default not check version
		version = -1
	}
	if err := conn.Delete(args[0], version); err != nil {
		fmt.Println("delete node failed:", err)
	} else {
		fmt.Println(args[0], "deleted")
	}
}

func cmdLs(conn *zk.Conn, args []string) {
	if len(args) < 1 {
		fmt.Println("arg invalid: ", args)
		return
	}

	if children, _, err := conn.Children(args[0]); err != nil {
		fmt.Println("get children failed: ", err)
		return
	} else {
		fmt.Println(children)
	}
}

func cmdGet(conn *zk.Conn, args []string) {
	if len(args) < 1 {
		fmt.Println("arg invalid: ", args)
		return
	}

	if data, status, err := conn.Get(args[0]); err != nil {
		fmt.Println("get node information failed: ", err)
		return
	} else {
		fmt.Println("data:", string(data))
		showNodeStatus(status)
	}
}

func cmdSet(conn *zk.Conn, args []string) {
	if len(args) < 2 {
		fmt.Println("arg invalid: ", args)
		return
	}

	version := int32(-1)
	if len(args) == 3 {
		value, _ := strconv.ParseInt(args[2], 10, 32)
		version = int32(value)
	}
	if status, err := conn.Set(args[0], []byte(args[1]), version); err != nil {
		fmt.Println("set node data failed: ", err)
		return
	} else {
		showNodeStatus(status)
	}
}

func cmdGetAcl(conn *zk.Conn, args []string) {
	if len(args) < 1 {
		fmt.Println("arg invalid: ", args)
		return
	}

	if acls, _, err := conn.GetACL(args[0]); err != nil {
		fmt.Println("getacl failed: ", err)
		return
	} else {
		fmt.Println(acls)
	}
}

func cmdSetAcl(conn *zk.Conn, args []string) {
	if len(args) < 2 {
		fmt.Println("arg invalid: ", args)
		return
	}

	var acl []zk.ACL
	aclStrs := strings.Split(args[1], ":")
	switch aclStrs[0] { // todo
	case "digest":
		acl = zk.DigestACL(zk.PermAll, aclStrs[1], aclStrs[2])
	}

	if status, err := conn.SetACL(args[0], acl, -1); err != nil {
		fmt.Println("setacl failed: ", err)
		return
	} else {
		showNodeStatus(status)
	}
}

func main() {
	// list args for debug
	args := os.Args
	for _, value := range args {
		fmt.Println(value)
	}
	fmt.Println("=========== golang zkCli ===========")

	zkServerAddr := ""
	if len(args) == 1 { // default connect to localhost zk server
		zkServerAddr = "127.0.1:2181"
	} else {
		if len(args) < 3 {
			fmt.Println("args not enough. try -server host:port")
			return
		}
		switch args[1] {
		case "-server":
			zkServerAddr = args[2]
		default:
			fmt.Println("unsupport command. try -server host:port")
			return
		}
	}

	// connect to zk server
	conn, zkEvent, err := zk.Connect([]string{zkServerAddr}, time.Second*3)
	if err != nil {
		fmt.Printf("connect to %s failed. error: %v", zkServerAddr, err)
	}
	go zkEventWatcher(zkEvent)

	// parse user input command
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf(">")
		data, _, _ := reader.ReadLine()
		command := string(data)
		if command == "quit" {
			break
		}
		commandHandler(conn, command)
	}

	fmt.Println("Bye bye!")
}
