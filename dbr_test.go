package samplequerybuilder

import (
	"log"
	"testing"
	"time"

	"github.com/gocraft/dbr"
)

func createDbrConn() *dbr.Connection {
	conn, err := dbr.Open("postgres", "user=pgtest dbname=pgtest sslmode=disable", nil)
	if err != nil {
		log.Fatalf("failed to open: %s", err)
	}
	return conn
}

func insertDataDbrValues(conn *dbr.Connection) error {
	sess := conn.NewSession(nil)
	t1, err := time.Parse("2006-01-02", "1985-12-31")
	t2, err := time.Parse("2006-01-02", "1987-10-31")
	t3, err := time.Parse("2006-01-02", "1985-11-10")
	result, err := sess.InsertInto("account").
		Columns("name", "dob").
		Values("8maki", t1).
		Values("moqada", t2).
		Values("ideyuta", t3).
		Exec()
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	log.Printf("%d rows created\n", count)
	result, err = sess.InsertInto("note").
		Columns("account_id", "title", "body").
		Values(1, "test title 01", "test body").
		Values(1, "test title 02", "test body").
		Values(1, "test title 03", "test body").
		Values(2, "test title 01", "test body").
		Exec()
	if err != nil {
		return err
	}
	count, _ = result.RowsAffected()
	log.Printf("%d rows created\n", count)

	return nil
}

func insertDataDbrRecord(conn *dbr.Connection) error {
	sess := conn.NewSession(nil)
	t1, err := time.Parse("2006-01-02", "1985-12-31")
	yamaki := Account{
		Name: "8maki",
		Dob:  t1,
	}
	t2, err := time.Parse("2006-01-02", "1987-10-31")
	ideyuta := Account{
		Name: "ideyuta",
		Dob:  t2,
	}
	t3, err := time.Parse("2006-01-02", "1985-11-10")
	moqada := Account{
		Name: "moqada",
		Dob:  t3,
	}
	result, err := sess.InsertInto("account").
		Columns("name", "dob").
		Record(yamaki).
		Record(moqada).
		Record(ideyuta).
		Exec()
	if err != nil {
		return err
	}
	count, _ := result.RowsAffected()
	log.Printf("%d rows created\n", count)

	note1 := Note{
		AccountID: 1,
		Title:     dbr.NewNullString("test title 01"),
		Body:      dbr.NewNullString("test body"),
	}
	note2 := Note{
		AccountID: 1,
		Title:     dbr.NewNullString("test title 02"),
		Body:      dbr.NewNullString("test body"),
	}
	note3 := Note{
		AccountID: 1,
		Title:     dbr.NewNullString("test title 03"),
		Body:      dbr.NewNullString("test body"),
	}
	note4 := Note{
		AccountID: 2,
		Title:     dbr.NewNullString("test title 01"),
		Body:      dbr.NewNullString("test body"),
	}
	result, err = sess.InsertInto("note").
		Columns("account_id", "title", "body").
		Record(note1).
		Record(note2).
		Record(note3).
		Record(note4).
		Exec()
	if err != nil {
		return err
	}
	count, _ = result.RowsAffected()
	log.Printf("%d rows created\n", count)

	return nil
}

func TestDbrPingDB(t *testing.T) {
	conn := createDbrConn()
	db := createConn()
	err := db.Ping()
	if err != nil {
		t.Errorf("%v", conn)
		t.Errorf("ping failed: %s", err)
	}
}

func TestDbrInsertDataRecord(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}

	conn := createDbrConn()
	err = insertDataDbrRecord(conn)
	if err != nil {
		t.Errorf("failed to insert: %s", err)
	}
}

func TestDbrInsertDataValues(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}

	conn := createDbrConn()
	err = insertDataDbrValues(conn)
	if err != nil {
		t.Errorf("failed to insert: %s", err)
	}
}

func TestDbrSelectData(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}

	conn := createDbrConn()
	err = insertDataDbrValues(conn)
	if err != nil {
		t.Errorf("failed to insert: %s", err)
	}

	sess := conn.NewSession(nil)
	var accounts []Account
	sess.Select("id, name, dob").
		From("account").
		LoadStructs(&accounts)
	for _, account := range accounts {
		t.Logf("%v", account)
	}
}

func TestDbrSelectJoinData(t *testing.T) {
	db := createConn()
	err := setUp(db)
	defer tearDown(db)
	if err != nil {
		t.Errorf("failed to create table: %s", err)
	}

	conn := createDbrConn()
	err = insertDataDbrValues(conn)
	if err != nil {
		t.Errorf("failed to insert: %s", err)
	}

	sess := conn.NewSession(nil)
	type NoteList struct {
		ID    dbr.NullInt64
		Title dbr.NullString
		Name  string
	}
	var notes []NoteList
	_, err = sess.Select("note.id, note.title, account.name").
		From("account").
		LeftJoin("note", "account.id = note.account_id").
		Load(&notes)
	if err != nil {
		t.Errorf("failed to select: %s", err)
	}
	for _, note := range notes {
		t.Logf("%v", note)
	}
}
