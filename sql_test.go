package sql_test

import (
	"database/sql"
	"log"
	"testing"

	_ "github.com/lib/pq"
)

func createDB() *sql.DB {
	db, err := sql.Open("postgres", "user=pgtest dbname=pgtest sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	return db
}

func createTable(db *sql.DB) error {
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

func dropTable(db *sql.DB) error {
	_, err := db.Exec(`
	DROP TABLE account;
	DROP TABLE note;
	`)
	if err != nil {
		return err
	}
	return nil
}

func insertData(db *sql.DB) error {
	_, err := db.Exec(`
	INSERT INTO account (name, dob) VALUES ('moqada', '1985/11/01');
	INSERT INTO account (name, dob) VALUES ('8maki', '1985/04/01');
	INSERT INTO account (name, dob) VALUES ('ideyuta', '1988/04/01');
	`)
	if err != nil {
		return err
	}
	return nil
}

func TestPingDB(t *testing.T) {
	db := createDB()
	err := db.Ping()
	if err != nil {
		t.Errorf("ping failed: %s", err)
	}
}

func TestCreateDropTable(t *testing.T) {
	db := createDB()
	err := createTable(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = dropTable(db)
	if err != nil {
		t.Errorf("failed to drop table: %s", err)
	}
}

func TestInsertData(t *testing.T) {
	db := createDB()
	err := createTable(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertData(db)
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}
	err = dropTable(db)
	if err != nil {
		t.Errorf("failed to drop table: %s", err)
	}
}

func TestSelectData(t *testing.T) {
	db := createDB()
	err := createTable(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertData(db)
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}

	var (
		id   int
		name string
	)
	rows, err := db.Query(`select id, name from account;`)
	if err != nil {
		t.Errorf("failed to select: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Errorf("failed scan row: %s", err)
		}
		t.Logf("id: %d, name: %s", id, name)
	}

	err = dropTable(db)
	if err != nil {
		t.Errorf("failed to drop table: %s", err)
	}
}
