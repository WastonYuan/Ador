package main



import (
    "bufio"
    "fmt"
    "io"
    "net"
    "time"
    "t_distributed/common"
	"t_util"
)

var Benchtype int

var RED *(t_util.Redirect)

func main() {
    Benchtype = common.YCSB
	RED = t_util.NewRedirect("coordinator.log")
	for i := 1; i <= 1000; i ++ {
		func(epoch_size int) {
    		var tcpAddr *net.TCPAddr
    		tcpAddr,_ = net.ResolveTCPAddr("tcp","localhost:9999")

    		conn,err := net.DialTCP("tcp",nil,tcpAddr)

    		if err!=nil {
    		    fmt.Println("Client connect error ! " + err.Error())
    		    return
    		}

    		defer conn.Close()

    		fmt.Println(conn.LocalAddr().String() + " : Client connected!")

    		onMessageReceived(conn, epoch_size)
			time.Sleep(2 * time.Second)
		}(i)
	}
}

func onMessageReceived(conn *net.TCPConn, epoch_size int) {
    totall_bytes := float64(0)
    start := time.Now()
    reader := bufio.NewReader(conn)
    // b := []byte(conn.LocalAddr().String() + " Say hello to Server... \n")
    // conn.Write(b)
    for i := 0 ; i < 10 ; i++ {
        
        var bench *(common.Benchmark)
        if Benchtype == common.YCSB {
            bench = common.NewYCSB(0.000001, 0.5, epoch_size)
        } else if Benchtype == common.TPCC {
            bench = common.NewTPCC(16, 0.5, epoch_size)
        }
        str := bench.Encode()
		b := []byte(str + "\n")
        totall_bytes = totall_bytes + float64(len(str))
        fmt.Printf("Send epoch %v with %v bytes\n", i, len(str))
        _, err := conn.Write(b)

        if err != nil {
            fmt.Println(err)
            break
        }

        msg, err := reader.ReadString('\n')
        fmt.Println(msg)

        if err != nil || err == io.EOF {
            fmt.Println(err)
            break
        }
        
    }
    defer func() {
        duration := time.Since(start)
        // fmt.Sprintf("duration:%v\ttotall bytes:%v M\n", duration, totall_bytes/1024/1024)
		RED.Write(fmt.Sprintf("duration:%v\ttotall bytes:%v M\n", duration, totall_bytes/1024/1024))
    }()
}