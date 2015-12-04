package sample_sql_tools

import (
	"database/sql"
	"log"

	_ "github.com/lib/pq"
)

func createConn() *sql.DB {
	db, err := sql.Open("postgres", "user=pgtest dbname=pgtest sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func setUp(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE account (
	  id SERIAL PRIMARY KEY
	  ,name TEXT NOT NULL
	  ,dob DATE NOT NULL
	);
	CREATE TABLE note (
	  id SERIAL PRIMARY KEY
	  ,account_id INTEGER NOT NULL
	  ,title TEXT NOT NULL
	  ,body TEXT NOT NULL
	);
	`)
	if err != nil {
		return err
	}
	return nil
}

func tearDown(db *sql.DB) error {
	_, err := db.Exec(`
	DROP TABLE account;
	DROP TABLE note;
	`)
	if err != nil {
		return err
	}
	return nil
}
