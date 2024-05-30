package db_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	inmemorydb "in_memory_db/db"
)

func TestInMemoryDBCommit(t *testing.T) {
	db := inmemorydb.NewInMemoryDB()
	db.Set("key1", "value1")
	db.StartTransaction()
	db.Set("key1", "value2")
	db.Commit()
	require.Equal(t, "value2", db.Get("key1"))
}

func TestInMemoryDBRollback(t *testing.T) {
	db := inmemorydb.NewInMemoryDB()
	db.Set("key1", "value1")
	db.StartTransaction()
	require.Equal(t, "value1", db.Get("key1"))
	db.Set("key1", "value2")
	require.Equal(t, "value2", db.Get("key1"))
	db.Rollback()
	require.Equal(t, "value1", db.Get("key1"))
}

func TestInMemoryDBRollback2(t *testing.T) {
	db := inmemorydb.NewInMemoryDB()
	db.StartTransaction()
	db.Set("key1", "value1")
	require.Equal(t, "value1", db.Get("key1"))
	db.Set("key1", "value2")
	require.Equal(t, "value2", db.Get("key1"))
	db.Rollback()
	// make sure that record doesn't exist anymore
	require.Equal(t, "", db.Get("key1"))
}

func TestInMemoryDBNested(t *testing.T) {
	db := inmemorydb.NewInMemoryDB()
	db.Set("key1", "value1")
	db.StartTransaction()
	db.Set("key1", "value2")
	require.Equal(t, "value2", db.Get("key1"))
	db.StartTransaction()
	require.Equal(t, "value2", db.Get("key1"))
	db.Delete("key1")
	db.Commit()
	require.Equal(t, "", db.Get("key1"))
	db.Commit()
	require.Equal(t, "", db.Get("key1"))
}

func TestInMemoryDBNestedRollback(t *testing.T) {
	db := inmemorydb.NewInMemoryDB()
	db.Set("key1", "value1")
	db.StartTransaction()
	db.Set("key1", "value2")
	require.Equal(t, "value2", db.Get("key1"))
	db.StartTransaction()
	require.Equal(t, "value2", db.Get("key1"))
	db.Delete("key1")
	db.Rollback()
	require.Equal(t, "value2", db.Get("key1"))
	db.Commit()
	require.Equal(t, "value2", db.Get("key1"))
}
