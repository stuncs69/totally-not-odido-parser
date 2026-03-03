package data

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

const indexVersion = "2"

type Store struct {
	db          *sql.DB
	datasetPath string
	datasetFile *os.File
}

type Record struct {
	RowNum         int64  `json:"row_num"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	Type           string `json:"type"`
	Status         string `json:"status"`
	Segment        string `json:"segment"`
	SalesChannel   string `json:"sales_channel"`
	BillingCity    string `json:"billing_city"`
	BillingState   string `json:"billing_state"`
	BillingCountry string `json:"billing_country"`
	PostalCode     string `json:"postal_code"`
	CreatedDate    string `json:"created_date"`
	ModifiedDate   string `json:"modified_date"`
	IsActive       bool   `json:"is_active"`
	IsDeleted      bool   `json:"is_deleted"`
}

type QueryParams struct {
	Q            string
	Type         string
	Status       string
	Country      string
	City         string
	Active       string
	ModifiedFrom string
	ModifiedTo   string
	Sort         string
	Limit        int
	Offset       int
}

type QueryResult struct {
	Records []Record `json:"records"`
	Total   int64    `json:"total"`
	Limit   int      `json:"limit"`
	Offset  int      `json:"offset"`
}

type CountBucket struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type Stats struct {
	TotalRows       int64         `json:"total_rows"`
	ActiveRows      int64         `json:"active_rows"`
	InactiveRows    int64         `json:"inactive_rows"`
	DeletedRows     int64         `json:"deleted_rows"`
	FirstModifiedAt string        `json:"first_modified_at"`
	LastModifiedAt  string        `json:"last_modified_at"`
	TopTypes        []CountBucket `json:"top_types"`
	TopStatuses     []CountBucket `json:"top_statuses"`
	TopCountries    []CountBucket `json:"top_countries"`
	TopCities       []CountBucket `json:"top_cities"`
}

type Facets struct {
	Types     []string `json:"types"`
	Statuses  []string `json:"statuses"`
	Countries []string `json:"countries"`
	Cities    []string `json:"cities"`
}

type Health struct {
	Ready       bool   `json:"ready"`
	DatasetPath string `json:"dataset_path"`
	DBPath      string `json:"db_path"`
	Rows        int64  `json:"rows"`
	IndexedAt   string `json:"indexed_at"`
}

type sourceRecord struct {
	ID           string `json:"Id"`
	Name         string `json:"Name"`
	Type         string `json:"Type"`
	AttrType     string `json:"attributes_type"`
	Status       string `json:"vlocity_cmt__Status__c"`
	Segment      string `json:"Segment__c"`
	SalesChannel string `json:"Sales_Channel__c"`

	BillingCity    string `json:"BillingCity"`
	BillingState   string `json:"BillingState"`
	BillingCountry string `json:"BillingCountry"`
	CountryCode    string `json:"CountryCode__c"`
	PostalCode     string `json:"BillingPostalCode"`

	CreatedDate  string `json:"CreatedDate"`
	ModifiedDate string `json:"LastModifiedDate"`
	IsActive     string `json:"IsActive"`
	IsDeleted    string `json:"IsDeleted"`
}

func NewStore(datasetPath, dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := applyPragmas(db); err != nil {
		return nil, err
	}

	file, err := os.Open(datasetPath)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:          db,
		datasetPath: datasetPath,
		datasetFile: file,
	}, nil
}

func (s *Store) Close() error {
	var errs []error
	if s.datasetFile != nil {
		errs = append(errs, s.datasetFile.Close())
	}
	if s.db != nil {
		errs = append(errs, s.db.Close())
	}
	return errors.Join(errs...)
}

func (s *Store) EnsureIndex(ctx context.Context) error {
	ok, err := s.isIndexCurrent(ctx)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	if err := s.rebuildIndex(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) Health(ctx context.Context) (Health, error) {
	var rows int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM records`).Scan(&rows); err != nil {
		return Health{}, err
	}

	indexedAt, _ := s.metaValue(ctx, "indexed_at")
	var dbPath string
	if err := s.db.QueryRowContext(ctx, `PRAGMA database_list`).Scan(new(int), new(string), &dbPath); err != nil {
		dbPath = "unknown"
	}

	return Health{
		Ready:       true,
		DatasetPath: s.datasetPath,
		DBPath:      dbPath,
		Rows:        rows,
		IndexedAt:   indexedAt,
	}, nil
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var out Stats

	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1),
		       COALESCE(SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN is_active = 0 THEN 1 ELSE 0 END), 0),
		       COALESCE(SUM(CASE WHEN is_deleted = 1 THEN 1 ELSE 0 END), 0),
		       COALESCE(MIN(NULLIF(modified_date, '')), ''),
		       COALESCE(MAX(NULLIF(modified_date, '')), '')
		FROM records`,
	).Scan(&out.TotalRows, &out.ActiveRows, &out.InactiveRows, &out.DeletedRows, &out.FirstModifiedAt, &out.LastModifiedAt)
	if err != nil {
		return Stats{}, err
	}

	out.TopTypes, err = s.topCounts(ctx, "type", 8)
	if err != nil {
		return Stats{}, err
	}
	out.TopStatuses, err = s.topCounts(ctx, "status", 8)
	if err != nil {
		return Stats{}, err
	}
	out.TopCountries, err = s.topCounts(ctx, "billing_country", 8)
	if err != nil {
		return Stats{}, err
	}
	out.TopCities, err = s.topCounts(ctx, "billing_city", 8)
	if err != nil {
		return Stats{}, err
	}

	return out, nil
}

func (s *Store) Facets(ctx context.Context, limit int) (Facets, error) {
	if limit <= 0 {
		limit = 100
	}

	types, err := s.distinctValues(ctx, "type", limit)
	if err != nil {
		return Facets{}, err
	}
	statuses, err := s.distinctValues(ctx, "status", limit)
	if err != nil {
		return Facets{}, err
	}
	countries, err := s.distinctValues(ctx, "billing_country", limit)
	if err != nil {
		return Facets{}, err
	}
	cities, err := s.distinctValues(ctx, "billing_city", limit)
	if err != nil {
		return Facets{}, err
	}

	return Facets{
		Types:     types,
		Statuses:  statuses,
		Countries: countries,
		Cities:    cities,
	}, nil
}

func (s *Store) QueryRecords(ctx context.Context, params QueryParams) (QueryResult, error) {
	if params.Limit <= 0 {
		params.Limit = 50
	}
	if params.Limit > 200 {
		params.Limit = 200
	}
	if params.Offset < 0 {
		params.Offset = 0
	}

	whereParts := []string{"1=1"}
	args := make([]any, 0, 8)

	if params.Type != "" {
		whereParts = append(whereParts, "r.type = ?")
		args = append(args, params.Type)
	}
	if params.Status != "" {
		whereParts = append(whereParts, "r.status = ?")
		args = append(args, params.Status)
	}
	if params.Country != "" {
		whereParts = append(whereParts, "r.billing_country = ?")
		args = append(args, params.Country)
	}
	if params.City != "" {
		whereParts = append(whereParts, "r.billing_city = ?")
		args = append(args, params.City)
	}
	if params.Active == "true" {
		whereParts = append(whereParts, "r.is_active = 1")
	}
	if params.Active == "false" {
		whereParts = append(whereParts, "r.is_active = 0")
	}
	if params.ModifiedFrom != "" {
		whereParts = append(whereParts, "r.modified_date >= ?")
		args = append(args, params.ModifiedFrom)
	}
	if params.ModifiedTo != "" {
		whereParts = append(whereParts, "r.modified_date <= ?")
		args = append(args, params.ModifiedTo)
	}

	ftsExpr := buildFTSExpr(params.Q)
	if ftsExpr != "" {
		whereParts = append(whereParts, "r.row_num IN (SELECT rowid FROM records_fts WHERE records_fts MATCH ?)")
		args = append(args, ftsExpr)
	}

	whereSQL := strings.Join(whereParts, " AND ")
	countSQL := "SELECT COUNT(1) FROM records r WHERE " + whereSQL

	var total int64
	if err := s.db.QueryRowContext(ctx, countSQL, args...).Scan(&total); err != nil {
		return QueryResult{}, err
	}

	orderBy := "r.modified_date DESC, r.row_num DESC"
	switch params.Sort {
	case "modified_asc":
		orderBy = "r.modified_date ASC, r.row_num ASC"
	case "name_asc":
		orderBy = "r.name ASC, r.row_num ASC"
	case "name_desc":
		orderBy = "r.name DESC, r.row_num DESC"
	case "city_asc":
		orderBy = "r.billing_city ASC, r.row_num ASC"
	case "city_desc":
		orderBy = "r.billing_city DESC, r.row_num DESC"
	}

	querySQL := `
		SELECT r.row_num, r.id, r.name, r.type, r.status, r.segment, r.sales_channel,
		       r.billing_city, r.billing_state, r.billing_country, r.postal_code,
		       r.created_date, r.modified_date, r.is_active, r.is_deleted
		FROM records r
		WHERE ` + whereSQL + `
		ORDER BY ` + orderBy + `
		LIMIT ? OFFSET ?`

	queryArgs := append(append(make([]any, 0, len(args)+2), args...), params.Limit, params.Offset)
	rows, err := s.db.QueryContext(ctx, querySQL, queryArgs...)
	if err != nil {
		return QueryResult{}, err
	}
	defer rows.Close()

	out := QueryResult{
		Records: make([]Record, 0, params.Limit),
		Total:   total,
		Limit:   params.Limit,
		Offset:  params.Offset,
	}

	for rows.Next() {
		var rec Record
		var activeInt int
		var deletedInt int
		if err := rows.Scan(
			&rec.RowNum,
			&rec.ID,
			&rec.Name,
			&rec.Type,
			&rec.Status,
			&rec.Segment,
			&rec.SalesChannel,
			&rec.BillingCity,
			&rec.BillingState,
			&rec.BillingCountry,
			&rec.PostalCode,
			&rec.CreatedDate,
			&rec.ModifiedDate,
			&activeInt,
			&deletedInt,
		); err != nil {
			return QueryResult{}, err
		}
		rec.IsActive = activeInt == 1
		rec.IsDeleted = deletedInt == 1
		out.Records = append(out.Records, rec)
	}

	return out, rows.Err()
}

func (s *Store) RecordJSON(ctx context.Context, rowNum int64) (json.RawMessage, error) {
	var offset int64
	var lineLen int64
	err := s.db.QueryRowContext(ctx, `SELECT file_offset, line_length FROM records WHERE row_num = ?`, rowNum).
		Scan(&offset, &lineLen)
	if err != nil {
		return nil, err
	}

	if lineLen <= 0 {
		return nil, fmt.Errorf("invalid line length %d for row %d", lineLen, rowNum)
	}

	buf := make([]byte, lineLen)
	n, err := s.datasetFile.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	raw := bytes.TrimRight(buf[:n], "\r\n")
	if !json.Valid(raw) {
		raw, _ = json.Marshal(map[string]string{"raw": string(raw)})
	}
	return append(json.RawMessage(nil), raw...), nil
}

func applyPragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA temp_store=MEMORY;",
		"PRAGMA cache_size=-200000;",
		"PRAGMA mmap_size=30000000000;",
	}

	for _, stmt := range pragmas {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) isIndexCurrent(ctx context.Context) (bool, error) {
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM sqlite_master
		WHERE type='table' AND name='metadata'`).Scan(&exists); err != nil {
		return false, err
	}
	if exists == 0 {
		return false, nil
	}

	size, err := s.metaValue(ctx, "source_size")
	if err != nil {
		return false, nil
	}
	mtime, err := s.metaValue(ctx, "source_mtime_unix")
	if err != nil {
		return false, nil
	}
	version, err := s.metaValue(ctx, "index_version")
	if err != nil {
		return false, nil
	}

	fileInfo, err := os.Stat(s.datasetPath)
	if err != nil {
		return false, err
	}

	if version != indexVersion {
		return false, nil
	}
	if size != strconv.FormatInt(fileInfo.Size(), 10) {
		return false, nil
	}
	if mtime != strconv.FormatInt(fileInfo.ModTime().Unix(), 10) {
		return false, nil
	}

	var recordsTable int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM sqlite_master
		WHERE type='table' AND name='records'`).Scan(&recordsTable); err != nil {
		return false, err
	}
	return recordsTable == 1, nil
}

func (s *Store) rebuildIndex(ctx context.Context) error {
	start := time.Now()
	fmt.Printf("rebuilding index from %s...\n", s.datasetPath)

	schemaSQL := `
		DROP TABLE IF EXISTS records;
		DROP TABLE IF EXISTS records_fts;
		DROP TABLE IF EXISTS metadata;

		CREATE TABLE records (
			row_num INTEGER PRIMARY KEY,
			id TEXT,
			name TEXT,
			type TEXT,
			status TEXT,
			segment TEXT,
			sales_channel TEXT,
			billing_city TEXT,
			billing_state TEXT,
			billing_country TEXT,
			postal_code TEXT,
			created_date TEXT,
			modified_date TEXT,
			is_active INTEGER NOT NULL,
			is_deleted INTEGER NOT NULL,
			file_offset INTEGER NOT NULL,
			line_length INTEGER NOT NULL
		);

		CREATE VIRTUAL TABLE records_fts USING fts5(
			id,
			name,
			billing_city,
			type,
			status,
			billing_country,
			tokenize = 'unicode61'
		);

		CREATE TABLE metadata (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);
	`
	if _, err := s.db.ExecContext(ctx, schemaSQL); err != nil {
		return err
	}

	f, err := os.Open(s.datasetPath)
	if err != nil {
		return err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 8*1024*1024)

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	commitBatch := func() error {
		if err := tx.Commit(); err != nil {
			return err
		}
		next, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		tx = next
		return nil
	}

	insertRecordSQL := `
		INSERT INTO records(
			row_num, id, name, type, status, segment, sales_channel,
			billing_city, billing_state, billing_country, postal_code,
			created_date, modified_date, is_active, is_deleted,
			file_offset, line_length
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	recStmt, err := tx.PrepareContext(ctx, insertRecordSQL)
	if err != nil {
		return err
	}
	defer recStmt.Close()

	const batchSize = 25000
	var lineNum int64
	var insertedRows int64
	var parseErrors int64
	var fileOffset int64

	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		if len(line) == 0 && readErr != nil {
			return readErr
		}

		lineNum++
		lineLen := int64(len(line))
		payload := bytes.TrimRight(line, "\r\n")

		var src sourceRecord
		if err := json.Unmarshal(payload, &src); err != nil {
			parseErrors++
			fileOffset += lineLen
			if readErr == io.EOF {
				break
			}
			continue
		}

		active := 0
		if parseBool(src.IsActive) {
			active = 1
		}
		deleted := 0
		if parseBool(src.IsDeleted) {
			deleted = 1
		}
		if src.Type == "" {
			src.Type = src.AttrType
		}
		if src.BillingCountry == "" {
			src.BillingCountry = src.CountryCode
		}

		if _, err := recStmt.ExecContext(
			ctx,
			lineNum,
			src.ID,
			src.Name,
			src.Type,
			src.Status,
			src.Segment,
			src.SalesChannel,
			src.BillingCity,
			src.BillingState,
			src.BillingCountry,
			src.PostalCode,
			src.CreatedDate,
			src.ModifiedDate,
			active,
			deleted,
			fileOffset,
			lineLen,
		); err != nil {
			return err
		}
		insertedRows++

		fileOffset += lineLen

		if lineNum%batchSize == 0 {
			if err := recStmt.Close(); err != nil {
				return err
			}
			if err := commitBatch(); err != nil {
				return err
			}

			recStmt, err = tx.PrepareContext(ctx, insertRecordSQL)
			if err != nil {
				return err
			}

			fmt.Printf("indexed %,d rows...\n", lineNum)
		}

		if readErr == io.EOF {
			break
		}
	}

	if err := recStmt.Close(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	// Bulk-load FTS in one statement to avoid per-row virtual table write overhead.
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO records_fts(rowid, id, name, billing_city, type, status, billing_country)
		SELECT row_num, id, name, billing_city, type, status, billing_country
		FROM records`); err != nil {
		return err
	}

	indexSQL := `
		CREATE INDEX idx_records_id ON records(id);
		CREATE INDEX idx_records_type ON records(type);
		CREATE INDEX idx_records_status ON records(status);
		CREATE INDEX idx_records_country ON records(billing_country);
		CREATE INDEX idx_records_city ON records(billing_city);
		CREATE INDEX idx_records_modified ON records(modified_date);
		CREATE INDEX idx_records_active ON records(is_active);
	`
	if _, err := s.db.ExecContext(ctx, indexSQL); err != nil {
		return err
	}

	fileInfo, err := os.Stat(s.datasetPath)
	if err != nil {
		return err
	}

	meta := map[string]string{
		"index_version":     indexVersion,
		"source_size":       strconv.FormatInt(fileInfo.Size(), 10),
		"source_mtime_unix": strconv.FormatInt(fileInfo.ModTime().Unix(), 10),
		"indexed_at":        time.Now().UTC().Format(time.RFC3339),
		"row_count":         strconv.FormatInt(insertedRows, 10),
		"parse_errors":      strconv.FormatInt(parseErrors, 10),
	}

	metaTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := metaTx.PrepareContext(ctx, `INSERT INTO metadata(key, value) VALUES(?, ?)`)
	if err != nil {
		return err
	}
	for k, v := range meta {
		if _, err := stmt.ExecContext(ctx, k, v); err != nil {
			stmt.Close()
			metaTx.Rollback()
			return err
		}
	}
	if err := stmt.Close(); err != nil {
		metaTx.Rollback()
		return err
	}
	if err := metaTx.Commit(); err != nil {
		return err
	}

	fmt.Printf("index rebuild complete in %s (rows=%d, parse_errors=%d)\n", time.Since(start).Round(time.Second), lineNum, parseErrors)
	return nil
}

func (s *Store) metaValue(ctx context.Context, key string) (string, error) {
	var value string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM metadata WHERE key = ?`, key).Scan(&value); err != nil {
		return "", err
	}
	return value, nil
}

func (s *Store) topCounts(ctx context.Context, field string, limit int) ([]CountBucket, error) {
	column, ok := map[string]string{
		"type":            "type",
		"status":          "status",
		"billing_country": "billing_country",
		"billing_city":    "billing_city",
	}[field]
	if !ok {
		return nil, fmt.Errorf("unsupported field %q", field)
	}

	query := fmt.Sprintf(`
		SELECT %s AS value, COUNT(1) AS count
		FROM records
		WHERE COALESCE(%s, '') <> ''
		GROUP BY %s
		ORDER BY count DESC, value ASC
		LIMIT ?`, column, column, column)

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]CountBucket, 0, limit)
	for rows.Next() {
		var b CountBucket
		if err := rows.Scan(&b.Value, &b.Count); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) distinctValues(ctx context.Context, field string, limit int) ([]string, error) {
	column, ok := map[string]string{
		"type":            "type",
		"status":          "status",
		"billing_country": "billing_country",
		"billing_city":    "billing_city",
	}[field]
	if !ok {
		return nil, fmt.Errorf("unsupported field %q", field)
	}

	query := fmt.Sprintf(`
		SELECT %s AS value
		FROM records
		WHERE COALESCE(%s, '') <> ''
		GROUP BY %s
		ORDER BY COUNT(1) DESC, value ASC
		LIMIT ?`, column, column, column)

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

func parseBool(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "y":
		return true
	default:
		return false
	}
}

func buildFTSExpr(query string) string {
	query = strings.TrimSpace(query)
	if query == "" {
		return ""
	}

	parts := strings.FieldsFunc(query, func(r rune) bool {
		return !(r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z'))
	})
	if len(parts) == 0 {
		return ""
	}

	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			continue
		}
		clean = append(clean, part+"*")
	}
	return strings.Join(clean, " AND ")
}
