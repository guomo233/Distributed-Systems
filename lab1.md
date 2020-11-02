# 设计

* worker 单方面的向 Master 发送消息；
* 如果一个 worker 没有在合理时间内（10s）完成任务就算故障；

# Master

main/mrmaster.go 是 Master 的驱动代码，通过命令行参数传入输入文件：
```go
import "../mr"

func main() {
  // ...
	// 创建一个 Master 并等到其运行结束
	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	
	time.Sleep(time.Second)
}
```
Master 实现于 mr/master.go：
```go

```
