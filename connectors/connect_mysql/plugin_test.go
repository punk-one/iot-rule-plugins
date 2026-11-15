package main

import (
	"database/sql"
	mysql "github.com/go-sql-driver/mysql"
	"testing"
)

func TestMySQLTLSAndNamedBinding(t *testing.T) {
	cfg, err := mysql.ParseDSN(connectionString(config{Host: "db.invalid", Port: 3306, Database: "demo", Username: "user", Password: "synthetic", TimeoutMS: 1000}))
	if err != nil || cfg.TLSConfig != "true" || cfg.MultiStatements || cfg.InterpolateParams {
		t.Fatalf("unsafe driver config: %v", err)
	}
	query, args, err := bindNamed("SELECT ':ignore',`literal:column` FROM t WHERE id=:id AND value=:v -- :ignored\n", []any{sql.Named("id", int64(1)), sql.Named("v", "x'; DROP TABLE t;--")})
	if err != nil || len(args) != 2 || args[1] != "x'; DROP TABLE t;--" {
		t.Fatalf("binding failed: %s %v", query, err)
	}
	if _, _, err = bindNamed("SELECT :missing", nil); err == nil {
		t.Fatal("missing parameter accepted")
	}
	if _, _, err = bindNamed("SELECT /*! SELECT :id */ 1", nil); err == nil {
		t.Fatal("executable comment accepted")
	}
}
