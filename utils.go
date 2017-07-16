package samplequerybuilder

import (
	"database/sql"
	"log"
	"testing"
	"time"

	"github.com/gocraft/dbr"
	_ "github.com/lib/pq" // postgres
)

// Account account
type Account struct {
	ID   int64
	Name string
	Dob  time.Time
}

// Note note
type Note struct {
	ID        int64
	AccountID int64
	Title     dbr.NullString
	Body      dbr.NullString
}

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
	  ,title TEXT
	  ,body TEXT
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

func createTestData(t *testing.T, db *sql.DB) {
	_, err := db.Exec(`
	INSERT INTO account (name, dob) VALUES ('moqada', '1985/11/01');
	INSERT INTO account (name, dob) VALUES ('8maki', '1985/04/01');
	INSERT INTO account (name, dob) VALUES ('ideyuta', '1988/04/01');

	INSERT INTO note (account_id, title, body) VALUES (1, 'test title 01', 'test body');
	INSERT INTO note (account_id, title, body) VALUES (1, 'test title 02', 'test body');
	INSERT INTO note (account_id, title, body) VALUES (1, 'test title 03', 'test body');
	INSERT INTO note (account_id, title, body) VALUES (2, 'test title 01', 'test body');
	`)
	if err != nil {
		t.Fatal(err)
	}
}

func setupTestDB(t *testing.T) (*sql.DB, func()) {
	db, err := sql.Open("postgres", "user=pgtest dbname=pgtest sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
	DROP TABLE IF EXISTS account;
	DROP TABLE IF EXISTS note;

	CREATE TABLE account (
	  id SERIAL PRIMARY KEY
	  ,name TEXT NOT NULL
	  ,dob DATE NOT NULL
	);
	CREATE TABLE note (
	  id SERIAL PRIMARY KEY
	  ,account_id INTEGER NOT NULL
	  ,title TEXT
	  ,body TEXT
	);
	`)
	if err != nil {
		t.Fatal(err)
	}
	return db, func() {
		_, err := db.Exec(`
		DROP TABLE account;
		DROP TABLE note;
		`)
		if err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
}
