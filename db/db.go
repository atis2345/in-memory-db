package db

import "github.com/google/uuid"

type operationType int

const (
	operationSet    operationType = iota
	operationDelete operationType = iota
)

type transactionOperation struct {
	key, previousValue, transaction string
	operation                       operationType
}

func (to transactionOperation) operationType() operationType {
	return to.operation
}

func (to transactionOperation) getKey() string {
	return to.key
}

func (to transactionOperation) getPreviousValue() string {
	return to.previousValue
}

func (to transactionOperation) getTransaction() string {
	return to.transaction
}

type InMemoryDB struct {
	db              map[string]string
	transactionsLog []transactionOperation
	// transactionsTree binary tree. transactionUUID -> parentTransactionUUID
	transactionsTree map[string]string
	// activeTransaction activeTransactionUUID
	activeTransaction string
}

func NewInMemoryDB() InMemoryDB {
	return InMemoryDB{db: make(map[string]string), transactionsLog: []transactionOperation{}, transactionsTree: make(map[string]string)}
}

func (db *InMemoryDB) Get(key string) string {
	return db.db[key]
}

func (db *InMemoryDB) Set(key string, value string) {
	if db.activeTransaction == "" {
		db.StartTransaction()
		defer db.Commit()
	}
	db.transactionsLog = append(db.transactionsLog, transactionOperation{key: key, previousValue: db.db[key], transaction: db.activeTransaction, operation: operationSet})
	db.db[key] = value
}

func (db *InMemoryDB) Delete(key string) {
	if db.activeTransaction == "" {
		db.StartTransaction()
		defer db.Commit()
	}
	db.transactionsLog = append(db.transactionsLog, transactionOperation{key: key, previousValue: db.db[key], transaction: db.activeTransaction, operation: operationDelete})
	delete(db.db, key)
}

func (db *InMemoryDB) StartTransaction() {
	tr := uuid.NewString()
	db.transactionsTree[tr] = db.activeTransaction
	db.activeTransaction = tr
}

// Commit remove outdated logs and replace activeTransaction by parent
func (db *InMemoryDB) Commit() {
	if db.activeTransaction == "" {
		return
	}

	tr := db.activeTransaction

	for i := len(db.transactionsLog) - 1; i >= 0; i-- {
		if db.transactionsLog[i].getTransaction() == tr {
			db.transactionsLog = db.transactionsLog[:i]
		}
	}

	db.activeTransaction = db.transactionsTree[db.activeTransaction]
	delete(db.transactionsTree, tr)
}

// Rollback perform rollback by LIFO order. Remove outdated logs and replace activeTransaction by parent
func (db *InMemoryDB) Rollback() {
	tr := db.activeTransaction
	if tr == "" {
		return
	}

	for i := len(db.transactionsLog) - 1; i >= 0; i-- {
		tl := db.transactionsLog[i]
		if tl.getTransaction() != db.activeTransaction {
			continue
		}
		key := tl.getKey()
		value := tl.getPreviousValue()
		// we have to revert value if it did exist before
		if value != "" {
			db.db[key] = value
		}
		// we have to delete record if it didn't exist before
		if value == "" && tl.operationType() == operationSet {
			delete(db.db, key)
		}

		db.transactionsLog = db.transactionsLog[:i]
	}

	db.activeTransaction = db.transactionsTree[tr]
	delete(db.transactionsTree, tr)
}
