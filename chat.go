package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/tencentyun/scf-go-lib/cloudfunction"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

// 注册
// 收到数据
// 断开连接
// 推送数据
func handler(ctx context.Context, data map[string]interface{}) (response map[string]interface{}, globalErr error) {
	fmt.Println(data)
	globalErr = nil
	response = make(map[string]interface{})
	if websocketData, ok := data["websocket"]; ok {
		if websocketData, ok := websocketData.(map[string]interface{}); ok {
			if checkWebsocket(websocketData) {
				goto handle
			}
		}
	}
	response["errNo"] = 1
	response["errMsg"] = "error"
	return
handle:
	websocketData := data["websocket"].(map[string]interface{})
	switch action := websocketData["action"].(string); action {
	case "connecting":
		fmt.Println("handle connection")
		response["errNo"] = 0
		response["errMsg"] = "ok"
		websocketResponse := make(map[string]interface{})
		websocketResponse["action"] = "connecting"
		websocketResponse["secConnectionID"] = websocketData["secConnectionID"]
		response["websocket"] = websocketResponse
		addConnection(websocketData["secConnectionID"].(string))
		return
	case "data send":
		fmt.Println("handle data send")
		connections, _ := QueryOnlineConnection()
		for _, connectionId := range connections {
			response["errNo"] = 0
			response["errMsg"] = "ok"
			websocketResponse := make(map[string]interface{})
			websocketResponse["action"] = "data send"
			websocketResponse["secConnectionID"] = connectionId
			websocketResponse["dataType"] = "text"
			websocketResponse["data"] = websocketData["data"]
			response["websocket"] = websocketResponse
			resp, err := post(response)
			fmt.Println(("Post end"))
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			defer resp.Body.Close()
			body, _ := ioutil.ReadAll(resp.Body)
			fmt.Println("Post response: ", string(body))
		}
		response["errNo"] = 0
		response["errMsg"] = "ok"
		websocketResponse := make(map[string]interface{})
		websocketResponse["action"] = "data send"
		websocketResponse["secConnectionID"] = websocketData["secConnectionID"]
		websocketResponse["dataType"] = "text"
		websocketResponse["data"] = websocketData["data"]
		response["websocket"] = websocketResponse
		return
	case "data recv":
		fmt.Println("handle data recv")
		response["errNo"] = 0
		response["errMsg"] = "ok"
		websocketResponse := make(map[string]interface{})
		websocketResponse["action"] = "data send"
		websocketResponse["secConnectionID"] = websocketData["secConnectionID"]
		websocketResponse["dataType"] = "text"
		websocketResponse["data"] = websocketData["data"]
		response["websocket"] = websocketResponse
		return
	case "closing":
		fmt.Println("handle closing")
		response["errNo"] = 0
		response["errMsg"] = "ok"
		websocketResponse := make(map[string]interface{})
		websocketResponse["action"] = "closing"
		websocketResponse["secConnectionID"] = websocketData["secConnectionID"]
		response["websocket"] = websocketResponse
		removeConnection(websocketData["secConnectionID"].(string))
		return
	default:
		fmt.Println("unknown action: ", action)
		response["errNo"] = 1
		response["errMsg"] = "error"
		return
	}
}

func checkRequestContext(requestContext map[string]interface{}) (result bool){
	fmt.Println("path: ", requestContext["path"].(string))
	//if v, ok := requestContext["websocketEnable"]; ok {
	//	// 判断是否 websocketEnable: true
	//	ifEnable, ok := v.(bool)
	//	if !ok  || !ifEnable{
	//		return false
	//	}
	//} else {
	//	return false
	//}
	return true
}

func checkWebsocket(websocketData map[string]interface{}) (result bool) {
	if v, ok := websocketData["action"]; ok {
		fmt.Println("action: ", v.(string))
	} else {
		return false
	}
	if v, ok := websocketData["secConnectionID"]; ok {
		fmt.Println("secConnectionID: ", v.(string))
	} else {
		return false
	}
	return true
}

func addConnection(connectionId string) (result bool) {
	// 首先查询connectionId是否存在
	var connectionRecord ConnectionRecord
	exist, _ := connectionRecord.Query(connectionId)
	if exist {
		fmt.Println("Connection: ", connectionId, " exist")
		return false
	} else {
		fmt.Println("Add new connetion: ", connectionId)
		connectionRecord.ConnectionId = connectionId
		connectionRecord.ConnectTime = time.Now()
		connectionRecord.DisconnectTime = connectionRecord.ConnectTime
		connectionRecord.IsValid = true
		connectionRecord.Insert()
		fmt.Println("Add new connection success")
	}
	return true
}

func removeConnection(connectionId string) (result bool) {
	var connectionRecord ConnectionRecord
	exist, _ := connectionRecord.Query(connectionId)
	if !exist {
		fmt.Println("connection: ", connectionId, " not exist")
		return false
	}
	connectionRecord.DisconnectTime = time.Now()
	connectionRecord.IsValid = false
	connectionRecord.update()
	return true
}

func post(request map[string]interface{})(resp *http.Response, err error) {
	postUrl := os.Getenv("SEND_URL")
	request_str, _ := json.Marshal(request)
	fmt.Println("Post url: ", postUrl)
	fmt.Println("Post request: ", request_str)
	reader := bytes.NewReader(request_str)
	resp, err = http.Post(postUrl, "application/json", reader)
	return
}

func main() {
	cloudfunction.Start(handler)
}
