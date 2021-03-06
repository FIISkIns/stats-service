package main

import (
	"database/sql"
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
	"io/ioutil"
	"log"
	"net/http"
	"sort"
	"strconv"
	"time"
)

var database *sql.DB

func New(text string) error {
	return &errorString{text}
}

type errorString struct {
	s string
}

func (e *errorString) Error() string {
	return e.s
}

type ProgressItem struct {
	CourseId string `json:"courseId"`
	TaskId   string `json:"taskId"`
	Progress string `json:"progress"`
}

type StatsItem struct {
	StartedCourses   int    `json:"startedCourses"`
	CompletedCourses int    `json:"completedCourses"`
	LastLoggedIn     string `json:"lastLoggedIn"`
	TimeSpent        int    `json:"timeSpent"`
	LongestStreak    int    `json:"longestStreak"`
	CurrentStreak    int    `json:"-"`
}

func getCoursesProgress(userId string) (int, int, error) {
	resp, err := http.Get(config.CourseProgressServiceUrl + "/progress/" + userId)
	if err != nil {
		return 0, 0, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, 0, New("CourseProgressService: " + strconv.Itoa(resp.StatusCode) + "\nResponse: " + string(body))
	}
	progressItems := make([]ProgressItem, 0)
	err = json.Unmarshal(body, &progressItems)
	if err != nil {
		return 0, 0, err
	}
	sort.Slice(progressItems, func(i, j int) bool { return progressItems[i].CourseId < progressItems[j].CourseId })
	var noOfStartedCourses int
	var noOfCompletedCourses int
	var courseId string
	var started bool
	var completed bool
	for i, item := range progressItems {
		if courseId != item.CourseId || i == len(progressItems)-1 {
			if started {
				noOfStartedCourses++
			}
			if completed {
				noOfCompletedCourses++
			}
			courseId = item.CourseId
			started = false
			completed = true
		}
		if item.Progress != "not started" {
			started = true
		}
		if item.Progress != "completed" {
			completed = false
		}
	}
	return noOfStartedCourses, noOfCompletedCourses, nil
}

func getUserStats(userId string) (*StatsItem, error) {
	var stats StatsItem
	err := database.QueryRow("select currentStreak, longestStreak, lastLoggedIn, timeSpent from stats where userId = ?", userId).
		Scan(&stats.CurrentStreak, &stats.LongestStreak, &stats.LastLoggedIn, &stats.TimeSpent)
	if err != nil {
		return nil, err
	}
	noOfStartedCourses, noOfCompletedCourses, err := getCoursesProgress(userId)
	if err != nil {
		return nil, err
	}
	stats.StartedCourses = noOfStartedCourses
	stats.CompletedCourses = noOfCompletedCourses
	return &stats, nil
}

func getStatsHandler(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	if err := database.Ping(); err != nil {
		errorMessage := "Database error (unable to connect): " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	stats, err := getUserStats(ps.ByName("user"))
	if err != nil {
		if err == sql.ErrNoRows {
			http.Error(w, "User not found", 404)
		} else {
			errorMessage := "Cannot retrieve user stats: " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
		}

		return
	}
	jsonData, err := json.Marshal(stats)
	if err != nil {
		errorMessage := "JSON error: failed to marshal stats" + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonData)
}

func isSameDay(firstDate, secondDate time.Time) bool {
	if firstDate.Day() != secondDate.Day() {
		return false
	}
	if firstDate.Month() != secondDate.Month() {
		return false
	}
	if firstDate.Year() != secondDate.Year() {
		return false
	}
	return true
}

func isNextDay(firstDate, secondDate time.Time) bool {
	firstDate = firstDate.Add(time.Hour * 24)
	return isSameDay(firstDate, secondDate)
}

func pingPostHandler(w http.ResponseWriter, _ *http.Request, ps httprouter.Params) {
	if err := database.Ping(); err != nil {
		errorMessage := "Database error (unable to connect): " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	userId := ps.ByName("user")
	stats, err := getUserStats(userId)
	if err != nil {
		if err == sql.ErrNoRows {
			stmt, err := database.Prepare("INSERT INTO stats(userId, longestStreak, currentStreak, lastLoggedIn , timeSpent)" +
				" VALUES(?, ?, ?, ?, ?)")
			if err != nil {
				errorMessage := "SQL error (failed to prepare insert statement): " + err.Error()
				log.Println(errorMessage)
				http.Error(w, errorMessage, http.StatusInternalServerError)
				return
			}
			_, err = stmt.Exec(userId, 1, 1, time.Now().UTC().Format(time.RFC3339), 0)
			if err != nil {
				errorMessage := "Database error (failed to insert into stats): " + err.Error()
				log.Println(errorMessage)
				http.Error(w, errorMessage, http.StatusInternalServerError)
				return
			}
		} else {
			errorMessage := "Cannot retrieve user stats: " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
			return
		}
	} else {
		currentTime := time.Now().UTC()
		lastLoggedIn, err := time.Parse(time.RFC3339, stats.LastLoggedIn)
		if err != nil {
			errorMessage := "Database error: lastLoggedIn wrong format\nNeeded format RFC3339: " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
			return
		}
		if isSameDay(lastLoggedIn, currentTime) {
			//update timeSpent and lastLoggedIn
			duration := int(currentTime.Sub(lastLoggedIn).Seconds())
			if duration < 3600 {
				stats.TimeSpent += duration
			}
		} else if isNextDay(lastLoggedIn, currentTime) {
			//update currentStreak, possibly update longestStreak
			stats.CurrentStreak++
			if stats.CurrentStreak > stats.LongestStreak {
				stats.LongestStreak = stats.CurrentStreak
			}
		} else {
			stats.CurrentStreak = 0
		}
		//update lastLoggedIn
		stats.LastLoggedIn = currentTime.Format(time.RFC3339)
		stmt, err := database.Prepare("UPDATE stats SET currentStreak = ?, longestStreak = ?, lastLoggedIn = ?, timeSpent = ? where userId = ?")
		if err != nil {
			errorMessage := "SQL error (cannot prepare update statement): " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
			return
		}
		_, err = stmt.Exec(stats.CurrentStreak, stats.LongestStreak, stats.LastLoggedIn, stats.TimeSpent, userId)
		if err != nil {
			errorMessage := "Database error (failed to update stats): " + err.Error()
			log.Println(errorMessage)
			http.Error(w, errorMessage, http.StatusInternalServerError)
			return
		}
	}

}

func initDatabase() error {
	if err := database.Ping(); err != nil {
		return err
	}
	var auxiliary int
	err := database.QueryRow("SHOW TABLES LIKE 'stats';").Scan(&auxiliary)
	if err != nil && err == sql.ErrNoRows {
		stmt, err := database.Prepare("create table stats (" +
			"userId varchar(100) primary key," +
			"longestStreak int not null," +
			"currentStreak int not null," +
			"lastLoggedIn varchar(20) not null," +
			"timeSpent int not null)")
		if err != nil {
			return err
		}
		_, err = stmt.Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func checkHealth(w http.ResponseWriter, url string) bool {
	resp, err := http.Get(url)
	if err != nil {
		errorMessage := "Failed to communicate with: " + url + "\nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return false
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		errorMessage := "Failed to read response from: " + url + "\nCause: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return false
	}
	if (resp.StatusCode / 100) != 2 {
		errorMessage := "Failed health check on: " + url + "\nResponse: " + string(body)
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return false
	}
	return true
}

func healthCheckHandler(w http.ResponseWriter, _ *http.Request, _ httprouter.Params) {
	if err := database.Ping(); err != nil {
		errorMessage := "Database connection failed: " + err.Error()
		log.Println(errorMessage)
		http.Error(w, errorMessage, http.StatusInternalServerError)
		return
	}
	if success := checkHealth(w, config.CourseProgressServiceUrl+"/health"); !success {
		return
	}
}

func main() {
	initConfig()
	database, _ = sql.Open("mysql", config.DatabaseUrl)
	if err := initDatabase(); err != nil {
		log.Fatal(err)
	}
	defer database.Close()
	router := httprouter.New()
	router.GET("/health", healthCheckHandler)
	router.HEAD("/health", healthCheckHandler)
	router.GET("/stats/:user", getStatsHandler)
	router.POST("/stats/:user/ping", pingPostHandler)
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(config.Port), router))
}
