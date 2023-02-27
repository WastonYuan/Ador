package main



import (
    "bufio"
    "fmt"
    "io"
    "net"
    "time"
    "t_distributed/common"
    "t_util"
    "sync"
)

var Benchtype int

var RED *(t_util.Redirect)

func main() {
    RED = t_util.NewRedirect("coordinator.log")
    t_util.InitConfigurationC()
    t_util.ReadJsonC("configure.json")
    fmt.Println(t_util.Cconf)

    if t_util.Cconf.Workload == "YCSB" {
        Benchtype = common.YCSB
    } else {
        Benchtype = common.TPCC
    }

    var wg sync.WaitGroup
    wg.Add(len(t_util.Cconf.Server))
    for i :=0 ;i < len(t_util.Cconf.Server); i++ {

        var tcpAddr *net.TCPAddr
        tcpAddr,_ = net.ResolveTCPAddr("tcp",t_util.Cconf.Server[i])
        conn,err := net.DialTCP("tcp",nil,tcpAddr)
        if err!=nil {
            fmt.Println("Client connect error ! " + err.Error())
            return
        }
        defer conn.Close()
        fmt.Println(conn.LocalAddr().String() + " : Client connected!")


        go onMessageReceived(conn, 500, &wg)

    }
    wg.Wait()
    
}

func onMessageReceived(conn *net.TCPConn, epoch_size int, wg *sync.WaitGroup) {
    totall_bytes := float64(0)
    start := time.Now()
    reader := bufio.NewReader(conn)
    // b := []byte(conn.LocalAddr().String() + " Say hello to Server... \n")
    // conn.Write(b)
    for i := 0 ; i < 10 ; i++ {
        
        var bench *(common.Benchmark)
        if Benchtype == common.YCSB {
            bench = common.NewYCSB(t_util.Cconf.Skew, t_util.Cconf.WriteRate, t_util.Cconf.EpochSize)
        } else if Benchtype == common.TPCC {
            bench = common.NewTPCC(t_util.Cconf.Warehouse, t_util.Cconf.NewOrderRate, t_util.Cconf.EpochSize)
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
        // fmt.Printf("duration:%v\ttotall bytes:%v M\n", duration, totall_bytes/1024/1024)
        RED.Write(fmt.Sprintf("duration:%v\ttotall bytes:%v M\n", duration, totall_bytes/1024/1024))
        wg.Done()
    }()
}