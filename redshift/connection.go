package redshift

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type RedshiftConn struct {
	url  string
	ctx  context.Context
	pool *pgxpool.Pool
}

// singleton redshift connection pool
var db *RedshiftConn

// create Redshift connection pool by using a password secret from AWS secret manager, e.g.
// secret, _ := getSecret("dev/ethdb/Redshift", "oocto", "us-west-2")
// pool, _ := Connect(secret, "dev", 10)
func Connect(secret *PasswordSecret, dbName string, poolSize int) (*RedshiftConn, error) {
	url := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?pool_max_conns=%d", secret.Username, secret.Password, secret.Host, secret.Port, dbName, poolSize)
	ctx := context.Background()
	pool, err := pgxpool.Connect(ctx, url)
	if err != nil {
		return nil, err
	}
	db = &RedshiftConn{url, ctx, pool}
	return db, nil
}

func (c *RedshiftConn) Close() {
	c.pool.Close()
}

func Close() {
	if db != nil {
		db.Close()
	}
}

// acquires a connection, executes query, then release the connection, e.g.
// rows, err := c.Query(`select name, age from users where age > $1`, 21)
// var name string
// var age int
// ScanRow(rows, &name, &age)
func (c *RedshiftConn) Query(sql string, args ...interface{}) (pgx.Rows, error) {
	return c.pool.Query(c.ctx, sql, args...)
}

// acquires a connection, executes query that returns at most one row, then close connection, e.g.,
// sql := `select age from users where name = $1`
// var age int
// c.QueryRow(sql, "John").Scan(&age)
// Note: if query returns more than 1 row, it may not release the connection.
//   It is safer to use Query followed by ScanRow to fetch the first row.
func (c *RedshiftConn) QueryRow(sql string, args ...interface{}) pgx.Row {
	return c.pool.QueryRow(c.ctx, sql, args...)
}

// fetch first row from query result, then close the resultset and release the connection.
func ScanRow(rows pgx.Rows, dst ...interface{}) (bool, error) {
	defer rows.Close()
	if rows.Next() {
		return true, rows.Scan(dst...)
	}
	return false, nil
}

// acquires a connection and executes query, scans result into struct array, then release connection, e.g.
// var users []*User
// c.Select(&users, `select id, name, email, age from users where age > $1`, 21)
// Note: this method is useful only if Go struct matches db columns so no data type conversion is required
func (c *RedshiftConn) Select(dst interface{}, query string, args ...interface{}) error {
	return pgxscan.Select(c.ctx, c.pool, dst, query, args...)
}

// acquires a connection and executes sql, then release the connection.
func (c *RedshiftConn) Exec(sql string, args ...interface{}) error {
	_, err := c.pool.Exec(c.ctx, sql, args...)
	return err
}

// acquires a connection and starts a transaction.
// must call Commit or Rollback to finalize tx and release the connection. e.g.,
// tx, _ := c.Begin()
// ctx := context.Background()
// tx.CopyFrom(ctx, ...)
// tx.Commit(ctx)
func (c *RedshiftConn) Begin() (pgx.Tx, error) {
	c.ctx = context.Background()
	return c.pool.Begin(c.ctx)
}

func (c *RedshiftConn) CopyFrom(tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	return c.pool.CopyFrom(c.ctx, tableName, columnNames, rowSrc)
}
