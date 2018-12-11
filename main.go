package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync"
	"time"

	"gopkg.in/yaml.v2"

	_ "github.com/go-sql-driver/mysql"
)

type dbConfig struct {
	Driver string
	Dsn    string
}

const period = 300

func main() {
	var config dbConfig

	source, err := ioutil.ReadFile("dbconf.yaml")
	if err != nil {
		log.Fatalf("readconfig error: %v", err)
	}

	err = yaml.Unmarshal(source, &config)
	if err != nil {
		log.Fatalf("yaml unmarshal error: %v", err)
	}

	fmt.Println("Parsing data started")

	db, err := sql.Open(config.Driver, config.Dsn)
	if err != nil {
		log.Fatalf("db error: %v", err)
	}

	pairs := map[int64]string{
		1: "BTC_ETH",
		2: "BTC_LTC",
		3: "BTC_ETC",
		4: "BTC_DASH",
	}

	wg := sync.WaitGroup{}

	for id, pair := range pairs {
		wg.Add(1)
		go func(id int64, pair string) {
			// https://www.epochconverter.com/
			start := int64(1483228800) // Sunday, January 1, 2017 6:00:00 AM GMT+06:00

			var lastRecordDate time.Time
			row := db.QueryRow(`
				SELECT created_at
				FROM market_history_poloniex
				WHERE pair_id = ?
				ORDER BY created_at DESC
				LIMIT 1
			`, id)
			err := row.Scan(&lastRecordDate)
			if err != nil {
				log.Printf("could not get last updated date: %v", err)
				start = time.Now().Add(-24 * 30 * time.Hour).Unix()
			} else {
				lastRecordDate = lastRecordDate.Add(5 * time.Minute)
				start = lastRecordDate.Unix()
			}

			count := 0

			url := fmt.Sprintf("https://poloniex.com/public?command=returnChartData&currencyPair=%s&start=%d&period=%d", pair, start, period)

			request, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Printf("request error: %v", err)
				wg.Done()
				return
			}

			response, err := http.DefaultClient.Do(request)
			if err != nil {
				log.Printf("request error: %v", err)
				wg.Done()
				return
			}
			defer response.Body.Close()

			var data []struct {
				Date          int64   `json:"date"`
				High          float64 `json:"high"`
				Low           float64 `json:"low"`
				Open          float64 `json:"open"`
				Close         float64 `json:"close"`
				Volume        float64 `json:"volume"`
				QuoteVolume   float64 `json:"quoteVolume"`
				WeightAverage float64 `json:"weightedAverage"`
			}
			if err := json.NewDecoder(response.Body).Decode(&data); err != nil {
				log.Printf("decode error: %v", err)
				wg.Done()
				return
			}
			for _, v := range data {
				if v.Date == 0 {
					continue
				}
				stmt, err := db.Prepare(`
					INSERT INTO market_history_poloniex (pair_id, low, high, avg, open, close, ask, bid, volume, quote_volume, created_at)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
				`)
				if err != nil {
					log.Printf("sql error: %v", err)
					continue
				}

				_, err = stmt.Exec(id, v.Low, v.High, v.WeightAverage, v.Open,
					v.Close, 9999, 0, v.QuoteVolume, v.Volume, time.Unix(v.Date, 0))
				if err != nil {
					stmt.Close()
					log.Printf("insert error: %v", err)
					continue
				}
				count++
				stmt.Close()
			}
			fmt.Printf("Inserted %d records for pair %s\n", count, pair)
			wg.Done()
		}(id, pair)
	}

	wg.Wait()

	if _, err := db.Exec(`TRUNCATE TABLE market_history`); err != nil {
		log.Printf("could not truncate market history: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO market_history SELECT * FROM market_history_poloniex`); err != nil {
		log.Printf("could not update market history: %v", err)
	}

	fmt.Println("The market_history table was completely updated")
}
