package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type ProgressItem struct {
	Course_id string `json:"course_id"`
	Progress string `json:"progress"`
	Task_id string `json:"task_id"`
	User_id string `json:"user_id"`
}

var database *sql.DB

func getJSON(sqlString string) ([]byte, error) {
	rows, err := database.Query(sqlString)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	count := len(columns)
	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)
	for rows.Next() {
		for i := 0; i < count; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}
	if len(tableData) == 0 {
		return nil, nil
	}
	jsonData, err := json.Marshal(tableData)
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}

func getUserStats(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	err := database.Ping()
	if err != nil {
		log.Fatal(err)
	}
	response, err := getJSON("select * from stats where user_id = " + ps.ByName("user"))
	if  response != nil{
		w.Header().Set("Content-Type", "application/json")
		w.Write(response)
	} else {
		http.Error(w, "User not found", 404)
	}
}

func handlePost(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	user := ps.ByName("user")
	response, err := getJSON("select * from stats where user_id = " + user)
	if err != nil {
		log.Fatal(err)
	}
	if response == nil {
		stmt, err := database.Prepare("INSERT INTO stats(user_id, courses_completed, longest_streak, started_courses, " +
			"last_logged_in, logged_in_since) VALUES(?, ?, ?, ?, ?, ?)")
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec(user, 0, 0, 0, nil, nil)
		if err != nil {
			log.Fatal(err)
		}
	}
	resp, err := http.Get("http://localhost:8182/" + user)
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
	}
	progressItems := make([]ProgressItem,0)
	json.Unmarshal(body, &progressItems)
	fmt.Fprintln(w, progressItems)
}

func main() {
	//user:password@protocol(host_ip:host_port)/database
	var err error
	database, err = sql.Open("mysql", "stats-service:mysqlpassword@tcp(127.0.0.1:3306)/statsdb")
	if err != nil{
		log.Fatal(err)
	}
	defer database.Close()
	router := httprouter.New()
	router.GET("/:user", getUserStats)
	router.POST("/:user/ping", handlePost)
	err = http.ListenAndServe(":8181", router)
	if err != nil {
		log.Fatal(err)
	}
}
