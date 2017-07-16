package samplequerybuilder

import (
	"testing"

	sq "github.com/elgris/sqrl"
)

var psq = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

func TestSqrlSimpleSelect(t *testing.T) {
	db, cleanup := setupTestDB(t)
	defer cleanup()

	q := psq.Select(
		"id",
		"name",
	).From("account").
		Where(sq.Eq{"name": "moqada"})
	sqlStr, args, err := q.ToSql()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("sql=%s", sqlStr)
	t.Logf("args=%+v", args)

	var ac Account
	err = db.QueryRow(sqlStr, args).Scan(&ac.ID, &ac.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", ac)
}
