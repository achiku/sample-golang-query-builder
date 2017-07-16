package samplequerybuilder

import (
	"database/sql"
	"testing"

	sq "github.com/Masterminds/squirrel"
)

var psql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func insertDataSquirrel(db *sql.DB) error {
	sql, args, err := psql.Insert("").
		Into("account").
		Columns("name", "dob").
		Values("moqada", "1985/11/01").
		Values("8maki", "1985/04/01").
		Values("ideyuta", "1985/04/01").
		ToSql()
	if err != nil {
		return err
	}
	_, err = db.Exec(sql, args...)
	if err != nil {
		return err
	}

	sql, args, err = psql.Insert("").
		Into("note").
		Columns("account_id", "title", "body").
		Values(1, "test title 01", "test body").
		Values(1, "test title 02", "test body").
		Values(1, "test title 03", "test body").
		Values(2, "test title 01", "test body").
		ToSql()
	if err != nil {
		return err
	}
	_, err = db.Exec(sql, args...)
	if err != nil {
		return err
	}

	return nil
}

func TestSquirrelPingDB(t *testing.T) {
	db := createConn()
	err := db.Ping()
	if err != nil {
		t.Errorf("ping failed: %s", err)
	}
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
	if err != nil {
		t.Errorf("failed to insert data: %s", err)
	}

	var (
		id   int
		name string
	)
	sql, _, _ := psql.Select("id, name").
		From("account").
		OrderBy("id DESC").
		ToSql()

	t.Logf("sql:%s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		t.Errorf("failed to select: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &name)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err)
		}
		t.Logf("%d, %s", id, name)
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

	// http://stackoverflow.com/questions/28642838/how-do-i-handle-nil-return-values-from-database
	// handling null string
	var (
		id    int
		name  string
		title sql.NullString
	)
	sql, _, _ := psql.Select("a.id, a.name, n.title").
		From("account a").
		LeftJoin("note n ON a.id = n.account_id").
		OrderBy("n.id DESC").
		ToSql()

	t.Logf("sql:%s", sql)
	rows, err := db.Query(sql)
	if err != nil {
		t.Errorf("failed to select: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &name, &title)
		if err != nil {
			t.Errorf("failed to scan row: %s", rows.Err())
			t.Errorf("failed to scan row: %s", err)
		}
		t.Logf("%d, %s, %s", id, name, title.String)
	}
}

func TestPlaceholderSelectDataSquirrel(t *testing.T) {
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

	userName := "moqada"
	var (
		id    int
		name  string
		title sql.NullString
	)
	sql, args, _ := psql.Select("a.id, a.name, n.title").
		From("account a").
		LeftJoin("note n ON a.id = n.account_id").
		Where(sq.Eq{"a.name": userName}).
		OrderBy("n.id DESC").
		ToSql()

	t.Logf("sql:%s", sql)
	t.Logf("arg:%s", args)
	rows, err := db.Query(sql, args...)
	if err != nil {
		t.Errorf("failed to select: %s", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&id, &name, &title)
		if err != nil {
			t.Errorf("failed to scan row: %s", rows.Err())
			t.Errorf("failed to scan row: %s", err)
		}
		t.Logf("%d, %s, %s", id, name, title.String)
	}
}
