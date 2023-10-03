// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package spanner

import (
	"context"
	"fmt"
	"strings"
	"time"

	surrealdb "github.com/surrealdb/surrealdb.go"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/pingcap/log"
)

const (
	surrealdbUri  = "surrealdb.uri"
	surrealdbUser = "surrealdb.user"
	surrealdbPass = "surrealdb.pass"
	surrealdbNs   = "surrealdb.ns"
	surrealdbDb   = "surrealdb.db"
)

type sdbCreator struct {
}

func (c sdbCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	return createDB(p)
}

func createDB(p *properties.Properties) (ycsb.DB, error) {
	dbUri := p.GetString(surrealdbUri, "ws://localhost:8000/rpc")
	dbUser := p.GetString(surrealdbUser, "")
	dbPass := p.GetString(surrealdbPass, "")
	useNs := p.GetString(surrealdbNs, "ycsb")
	useDb := p.GetString(surrealdbDb, "ycsb")

	return &surrealDB{
		dbUri,
		dbUser,
		dbPass,
		useNs,
		useDb,
	}, nil
}

func (s *surrealDB) connect() (*surrealdb.DB, error) {
	// Create client connection
	db, err := surrealdb.New(s.dbUri, surrealdb.WithTimeout(5*time.Second))
	if err != nil {
		return nil, err
	}

	// Signin
	if s.dbUser != "" {
		if _, err = db.Signin(map[string]string{
			"user": s.dbUser,
			"pass": s.dbPass,
		}); err != nil {
			return nil, err
		}
	}

	// Select namespace and database
	if _, err = db.Use(s.useNs, s.useDb); err != nil {
		return nil, err
	}

	return db, nil
}

type surrealDB struct {
	dbUri  string
	dbUser string
	dbPass string
	useNs  string
	useDb  string
}

type SurrealDBConnection struct{}

func (s *surrealDB) Close() error {
	return nil
}

func (s *surrealDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	db, err := s.connect()
	if err != nil {
		log.S().Panicf("Error connecting to SurrealDB endpoint: %w", err)
	}

	return context.WithValue(ctx, SurrealDBConnection{}, db)
}

func (s *surrealDB) CleanupThread(ctx context.Context) {
	ctx.Value(SurrealDBConnection{}).(*surrealdb.DB).Close()
}

// Read a document.
func (s *surrealDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	db := ctx.Value(SurrealDBConnection{}).(*surrealdb.DB)
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s:%s`, table, key)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s:%s`, strings.Join(fields, ","), table, key)
	}

	res, err := surrealdb.SmartUnmarshal[[]map[string][]byte](db.Query(query, nil))
	if err != nil {
		log.S().Errorf("Read error: %s", err.Error())
		return nil, fmt.Errorf("Read error: %s", err.Error())
	}

	return res[0], nil
}

// Scan documents.
func (s *surrealDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	db := ctx.Value(SurrealDBConnection{}).(*surrealdb.DB)
	var query string
	if len(fields) == 0 {
		query = fmt.Sprintf(`SELECT * FROM %s WHERE id > %s LIMIT %d`, table, startKey, count)
	} else {
		query = fmt.Sprintf(`SELECT %s FROM %s WHERE id > %s LIMIT %d`, strings.Join(fields, ","), table, startKey, count)
	}

	res, err := surrealdb.SmartUnmarshal[[]map[string][]byte](db.Query(query, nil))
	if err != nil {
		log.S().Errorf("Scan error: %s", err.Error())
		return nil, fmt.Errorf("Scan error: %s", err.Error())
	}

	return res, nil
}

// Insert a document.
func (s *surrealDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	db := ctx.Value(SurrealDBConnection{}).(*surrealdb.DB)
	_, err := db.Update(fmt.Sprintf(`%s:%s`, table, key), values)
	if err != nil {
		log.S().Errorf("Insert error: %s", err.Error())
		return fmt.Errorf("Insert error: %s", err.Error())
	}

	return nil
}

// Update a document.
func (s *surrealDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	db := ctx.Value(SurrealDBConnection{}).(*surrealdb.DB)
	_, err := db.Update(fmt.Sprintf(`%s:%s`, table, key), values)
	if err != nil {
		log.S().Errorf("Update error: %s", err.Error())
		return fmt.Errorf("Update error: %s", err.Error())
	}

	return nil
}

// Delete a document.
func (s *surrealDB) Delete(ctx context.Context, table string, key string) error {
	db := ctx.Value(SurrealDBConnection{}).(*surrealdb.DB)
	_, err := db.Delete(fmt.Sprintf(`%s:%s`, table, key))
	if err != nil {
		log.S().Errorf("Delete error: %s", err.Error())
		return fmt.Errorf("Delete error: %s", err.Error())
	}

	return nil
}

func init() {
	ycsb.RegisterDBCreator("surrealdb", sdbCreator{})
}
