package sample_sql_tools

import (
	"database/sql"
	"testing"
)

func insertDataSimple(db *sql.DB) error {
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
		return err
	}
	return nil
}

func TestSimplePingDB(t *testing.T) {
	db := createConn()
	err := db.Ping()
	if err != nil {
		t.Errorf("ping failed: %s", err)
	}
}

func TestSimpleCreateDropTable(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
}

func TestSimpleInsertData(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertDataSimple(db)
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}
}

func TestSimpleSelectData(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertDataSimple(db)
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}

	var (
		id   int
		name string
	)
	rows, err := db.Query(`SELECT id, name FROM account;`)
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
}

func TestSimplJoinSelectData(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Fatalf("failed to create table: %s", err)
	}

	var (
		userId     int
		titleCount int
	)

	var testData = []struct {
		userId     int
		titleCount int
	}{
		{1, 3},
		{2, 1},
		{3, 0},
	}

	for _, d := range testData {
		rows, err := db.Query(`
		SELECT
		  a.id
		  ,count(n.title)
		FROM account a
		LEFT OUTER JOIN note n
		ON a.id = n.account_id
		WHERE a.id = $1
		GROUP BY a.id
		`, d.userId)
		if err != nil {
			t.Fatalf("failed to select: %s", err)
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&userId, &titleCount)
			if err != nil {
				t.Fatalf("failed to scan row: %s")
			}
			if titleCount != d.titleCount {
				t.Errorf("expected %d, but got %d", d.titleCount, titleCount)
			}
		}
	}
}
