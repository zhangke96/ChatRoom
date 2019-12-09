package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"
)

var db *sql.DB

const (
	ConnectionRecordTable = "ConnectionRecord"
)
func connectDb() {
	if db != nil {
		fmt.Println("db connected")
		return
	}
	DB_HOST := os.Getenv("DB_HOST")
	DB_PORT := os.Getenv("DB_PORT")
	DB_USER := os.Getenv("DB_USER")
	DB_PASSWORD := os.Getenv("DB_PASSWORD")
	DB_DATABASE := os.Getenv("DB_DATABASE")

	connectStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=%v", DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_DATABASE, true)
	fmt.Println("connect db: ", connectStr)
	var err error
	db, err = sql.Open("mysql", connectStr)
	if err != nil {
		panic(err.Error())
	}
	return
}

type ConnectionRecord struct {
	Id int64 `json:"id"`
	ConnectionId string `json:"ConnectionID"`
	ConnectTime time.Time `json:"ConnectDate"`
	DisconnectTime time.Time `json:"DisconnectDate"`
	IsValid bool `json:"IsValid"`
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Query
func (record *ConnectionRecord)Query(ConnectionId string) (exist bool, err error) {
	connectDb()
	row := db.QueryRow("SELECT * FROM " + ConnectionRecordTable + " where ConnectionID = ?", ConnectionId)
	err = row.Scan(&record.Id, &record.ConnectionId, &record.ConnectTime, &record.DisconnectTime, &record.IsValid)
	if err == sql.ErrNoRows {
		exist = false
		err = nil
		fmt.Println("ERRNORows")
		return
	} else if err != nil {
		exist = false
		fmt.Println(err)
		return
	} else {
		exist = true
		return
	}
}

// Insert
func (record *ConnectionRecord)Insert() (err error) {
	connectDb()
	stmt, err := db.Prepare("INSERT INTO " + ConnectionRecordTable + " (ConnectionID, ConnectDate, DisconnectDate, IsValid) VALUES (?, ?, ?, ?)")
	checkErr(err)
	res, err := stmt.Exec(record.ConnectionId, record.ConnectTime, record.DisconnectTime, record.IsValid)
	checkErr(err)
	record.Id, _ = res.LastInsertId()
	return
}

// Update
func (record *ConnectionRecord)update() (err error) {
	connectDb()
	stmt, err := db.Prepare("UPDATE " + ConnectionRecordTable + " set ConnectionID = ?, ConnectDate = ?, DisconnectDate = ?, IsValid = ? where id = ?")
	checkErr(err)
	_, err = stmt.Exec(record.ConnectionId, record.ConnectTime, record.DisconnectTime, record.IsValid, record.Id)
	checkErr(err)
	fmt.Println("connection: ", record.ConnectionId, " update success")
	return
}

// QueryOnline
func QueryOnlineConnection() (connections []string, err error) {
	connectDb()
	rows, err := db.Query("SELECT ConnectionID FROM " + ConnectionRecordTable + " where IsValid = ?", true)
	defer rows.Close()
	if err != nil {
		checkErr(err)
		return
	}
	var connectionId string
	connections = []string{}
	for rows.Next() {
		err = rows.Scan(&connectionId)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		connections = append(connections, connectionId)
	}
	fmt.Println("alive connections num: ", len(connections))
	return
}

