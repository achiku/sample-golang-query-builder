package sample_sql_tools

import (
	"database/sql"
	"testing"

	sq "github.com/Masterminds/squirrel"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func TestSquirrelPingDB(t *testing.T) {
	db := createConn()
	err := db.Ping()
	if err != nil {
		t.Errorf("ping failed: %s", err)
	}
}

func insertDataSquirrel(db *sql.DB) error {
	sql, args, err := psql.Insert("account").
		Columns("name", "dob").
		Values("moqada", "1985/11/01").
		Values("8maki", "1985/04/01").
		Values("ideyuta", "1985/04/01").
		ToSql()
	if err != nil {
		return err
	}
	db.Exec(sql, args)

	sql, args, err = psql.Insert("note").
		Columns("account_id", "title", "body").
		Values(1, "test title 01", "test body").
		Values(1, "test title 02", "test body").
		Values(1, "test title 03", "test body").
		Values(2, "test title 01", "test body").
		ToSql()
	if err != nil {
		return err
	}
	db.Exec(sql, args)

	return nil
}

func TestInsertDataSquirrel(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertDataSquirrel(db)
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}
}

func TestSelectDataSquirrel(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertDataSquirrel(db)

	var (
		id   int
		name string
	)
	sql, _, _ := psql.Select("id, name").
		From("account").
		OrderBy("id DESC").
		ToSql()

	t.Logf("%s", sql)
	rows, err := db.Query(sql)
	t.Logf("%v", rows)
	if err != nil {
		t.Errorf("failed to select: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("failed to scan row: %s")
		}
		t.Logf("%s, %s", id, name)
	}
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}
}

func TestSelectJoinDataSquirrel(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}
	err = insertDataSquirrel(db)
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}
}
