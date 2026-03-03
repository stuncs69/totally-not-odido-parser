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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const indexVersion = "3"

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
	JSONPath     string
	JSONValue    string
	JSONOp       string
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

type AnalyticsField struct {
	Name  string `json:"name"`
	Label string `json:"label"`
}

type AnalyticsBucket struct {
	Value      string  `json:"value"`
	Count      int64   `json:"count"`
	Percentage float64 `json:"percentage"`
}

type AnalyticsDistribution struct {
	Field         string            `json:"field"`
	Filter        string            `json:"filter"`
	Limit         int               `json:"limit"`
	TotalRows     int64             `json:"total_rows"`
	MatchedRows   int64             `json:"matched_rows"`
	DistinctCount int64             `json:"distinct_count"`
	Buckets       []AnalyticsBucket `json:"buckets"`
}

type AnalyticsCountResult struct {
	Field string `json:"field"`
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type analyticsFieldSpec struct {
	Name  string
	Label string
	Expr  string
}

type jsonIndexedField struct {
	Path      string
	ValueText string
	ValueType string
}

var analyticsLabelOverrides = map[string]string{
	"id":         "ID",
	"is_active":  "Active Flag",
	"is_deleted": "Deleted Flag",
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
	if params.JSONPath != "" || params.JSONValue != "" {
		jsonParts := []string{"jf.row_num = r.row_num"}
		if params.JSONPath != "" {
			jsonParts = append(jsonParts, "jf.path = ?")
			args = append(args, params.JSONPath)
		}
		if params.JSONValue != "" {
			switch params.JSONOp {
			case "", "eq":
				jsonParts = append(jsonParts, "jf.value_text = ?")
				args = append(args, params.JSONValue)
			case "contains":
				jsonParts = append(jsonParts, "jf.value_text LIKE ?")
				args = append(args, "%"+params.JSONValue+"%")
			default:
				return QueryResult{}, fmt.Errorf("unsupported json_op %q", params.JSONOp)
			}
		}
		whereParts = append(whereParts, "EXISTS (SELECT 1 FROM record_json_fields jf WHERE "+strings.Join(jsonParts, " AND ")+")")
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
		DROP TABLE IF EXISTS record_json_fields;
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

		CREATE TABLE record_json_fields (
			row_num INTEGER NOT NULL,
			path TEXT NOT NULL,
			value_text TEXT NOT NULL,
			value_type TEXT NOT NULL
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

	insertJSONFieldSQL := `
		INSERT INTO record_json_fields(row_num, path, value_text, value_type)
		VALUES (?, ?, ?, ?)`

	const batchSize = 25000
	type parseTask struct {
		RowNum     int64
		FileOffset int64
		LineLen    int64
		Payload    []byte
	}
	type parseResult struct {
		RowNum     int64
		FileOffset int64
		LineLen    int64
		Src        sourceRecord
		Active     int
		Deleted    int
		Fields     []jsonIndexedField
		ParseError bool
	}
	type writeOutcome struct {
		InsertedRows int64
		ParseErrors  int64
		Err          error
	}

	ingestCtx, cancelIngest := context.WithCancel(ctx)
	defer cancelIngest()

	workerCount := indexParseWorkerCount()
	tasks := make(chan parseTask, workerCount*8)
	results := make(chan parseResult, workerCount*8)
	outcomes := make(chan writeOutcome, 1)

	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()

			localFields := make([]jsonIndexedField, 0, 32)
			for {
				select {
				case <-ingestCtx.Done():
					return
				case task, ok := <-tasks:
					if !ok {
						return
					}

					var doc any
					if err := json.Unmarshal(task.Payload, &doc); err != nil {
						select {
						case results <- parseResult{RowNum: task.RowNum, ParseError: true}:
						case <-ingestCtx.Done():
						}
						continue
					}

					obj, ok := doc.(map[string]any)
					if !ok {
						select {
						case results <- parseResult{RowNum: task.RowNum, ParseError: true}:
						case <-ingestCtx.Done():
						}
						continue
					}

					src := sourceRecord{
						ID:             anyToString(obj["Id"]),
						Name:           anyToString(obj["Name"]),
						Type:           anyToString(obj["Type"]),
						AttrType:       anyToString(obj["attributes_type"]),
						Status:         anyToString(obj["vlocity_cmt__Status__c"]),
						Segment:        anyToString(obj["Segment__c"]),
						SalesChannel:   anyToString(obj["Sales_Channel__c"]),
						BillingCity:    anyToString(obj["BillingCity"]),
						BillingState:   anyToString(obj["BillingState"]),
						BillingCountry: anyToString(obj["BillingCountry"]),
						CountryCode:    anyToString(obj["CountryCode__c"]),
						PostalCode:     anyToString(obj["BillingPostalCode"]),
						CreatedDate:    anyToString(obj["CreatedDate"]),
						ModifiedDate:   anyToString(obj["LastModifiedDate"]),
					}

					active := 0
					if parseAnyBool(obj["IsActive"]) {
						active = 1
					}
					deleted := 0
					if parseAnyBool(obj["IsDeleted"]) {
						deleted = 1
					}
					if src.Type == "" {
						src.Type = src.AttrType
					}
					if src.BillingCountry == "" {
						src.BillingCountry = src.CountryCode
					}

					localFields = localFields[:0]
					collectJSONIndexedFields("", doc, &localFields)
					fieldsCopy := append([]jsonIndexedField(nil), localFields...)

					res := parseResult{
						RowNum:     task.RowNum,
						FileOffset: task.FileOffset,
						LineLen:    task.LineLen,
						Src:        src,
						Active:     active,
						Deleted:    deleted,
						Fields:     fieldsCopy,
					}
					select {
					case results <- res:
					case <-ingestCtx.Done():
						return
					}
				}
			}
		}()
	}

	go func() {
		workers.Wait()
		close(results)
	}()

	go func() {
		var insertedRows int64
		var parseErrors int64
		var processedLines int64

		recStmt, err := tx.PrepareContext(ingestCtx, insertRecordSQL)
		if err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		jsonFieldStmt, err := tx.PrepareContext(ingestCtx, insertJSONFieldSQL)
		if err != nil {
			_ = recStmt.Close()
			outcomes <- writeOutcome{Err: err}
			return
		}

		closeStmts := func() error {
			if err := recStmt.Close(); err != nil {
				return err
			}
			if err := jsonFieldStmt.Close(); err != nil {
				return err
			}
			return nil
		}
		reopenStmts := func() error {
			var prepareErr error
			recStmt, prepareErr = tx.PrepareContext(ingestCtx, insertRecordSQL)
			if prepareErr != nil {
				return prepareErr
			}
			jsonFieldStmt, prepareErr = tx.PrepareContext(ingestCtx, insertJSONFieldSQL)
			if prepareErr != nil {
				return prepareErr
			}
			return nil
		}

		for res := range results {
			processedLines++
			if res.ParseError {
				parseErrors++
			} else {
				if _, err := recStmt.ExecContext(
					ingestCtx,
					res.RowNum,
					res.Src.ID,
					res.Src.Name,
					res.Src.Type,
					res.Src.Status,
					res.Src.Segment,
					res.Src.SalesChannel,
					res.Src.BillingCity,
					res.Src.BillingState,
					res.Src.BillingCountry,
					res.Src.PostalCode,
					res.Src.CreatedDate,
					res.Src.ModifiedDate,
					res.Active,
					res.Deleted,
					res.FileOffset,
					res.LineLen,
				); err != nil {
					cancelIngest()
					outcomes <- writeOutcome{Err: err}
					return
				}

				for _, field := range res.Fields {
					if _, err := jsonFieldStmt.ExecContext(ingestCtx, res.RowNum, field.Path, field.ValueText, field.ValueType); err != nil {
						cancelIngest()
						outcomes <- writeOutcome{Err: err}
						return
					}
				}
				insertedRows++
			}

			if processedLines%batchSize == 0 {
				if err := closeStmts(); err != nil {
					cancelIngest()
					outcomes <- writeOutcome{Err: err}
					return
				}
				if err := commitBatch(); err != nil {
					cancelIngest()
					outcomes <- writeOutcome{Err: err}
					return
				}
				if err := reopenStmts(); err != nil {
					cancelIngest()
					outcomes <- writeOutcome{Err: err}
					return
				}

				fmt.Printf("indexed %,d rows...\n", processedLines)
			}
		}

		if err := closeStmts(); err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		if err := tx.Commit(); err != nil {
			outcomes <- writeOutcome{Err: err}
			return
		}
		outcomes <- writeOutcome{
			InsertedRows: insertedRows,
			ParseErrors:  parseErrors,
		}
	}()

	var lineNum int64
	var fileOffset int64
	var readerErr error
	var writerOutcome writeOutcome
	writerOutcomeReady := false

readLoop:
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) == 0 && readErr == io.EOF {
			break
		}
		if len(line) == 0 && readErr != nil {
			readerErr = readErr
			cancelIngest()
			break
		}

		lineNum++
		lineLen := int64(len(line))
		payload := bytes.TrimRight(line, "\r\n")

		task := parseTask{
			RowNum:     lineNum,
			FileOffset: fileOffset,
			LineLen:    lineLen,
			Payload:    append([]byte(nil), payload...),
		}
		select {
		case tasks <- task:
		case writerOutcome = <-outcomes:
			writerOutcomeReady = true
			cancelIngest()
			break readLoop
		case <-ingestCtx.Done():
			break readLoop
		}

		fileOffset += lineLen

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			readerErr = readErr
			cancelIngest()
			break
		}
	}
	close(tasks)

	if !writerOutcomeReady {
		writerOutcome = <-outcomes
		writerOutcomeReady = true
	}

	if writerOutcome.Err != nil {
		_ = tx.Rollback()
		return writerOutcome.Err
	}
	if readerErr != nil {
		_ = tx.Rollback()
		return readerErr
	}

	insertedRows := writerOutcome.InsertedRows
	parseErrors := writerOutcome.ParseErrors

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
		CREATE INDEX idx_json_fields_path_value ON record_json_fields(path, value_text);
		CREATE INDEX idx_json_fields_value ON record_json_fields(value_text);
		CREATE INDEX idx_json_fields_row ON record_json_fields(row_num);
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

func (s *Store) JSONPaths(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT path
		FROM record_json_fields
		GROUP BY path
		ORDER BY COUNT(1) DESC, path ASC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]string, 0, limit)
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	return out, rows.Err()
}

func (s *Store) AnalyticsFields(ctx context.Context) ([]AnalyticsField, error) {
	specs, err := s.analyticsFieldSpecs(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]AnalyticsField, 0, len(specs))
	for _, spec := range specs {
		out = append(out, AnalyticsField{Name: spec.Name, Label: spec.Label})
	}
	return out, nil
}

func (s *Store) AnalyticsDistribution(ctx context.Context, field, filter string, limit int) (AnalyticsDistribution, error) {
	if limit <= 0 {
		limit = 25
	}
	if limit > 500 {
		limit = 500
	}

	spec, err := s.analyticsFieldSpec(ctx, field)
	if err != nil {
		return AnalyticsDistribution{}, err
	}

	filter = strings.TrimSpace(filter)
	whereSQL := ""
	filterArgs := make([]any, 0, 1)
	if filter != "" {
		whereSQL = " WHERE value LIKE ?"
		filterArgs = append(filterArgs, "%"+filter+"%")
	}

	sourceSQL := fmt.Sprintf("SELECT %s AS value FROM records", spec.Expr)

	var totalRows int64
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(1) FROM records`).Scan(&totalRows); err != nil {
		return AnalyticsDistribution{}, err
	}

	matchedSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM (%s) values_view%s",
		sourceSQL,
		whereSQL,
	)
	var matchedRows int64
	if err := s.db.QueryRowContext(ctx, matchedSQL, filterArgs...).Scan(&matchedRows); err != nil {
		return AnalyticsDistribution{}, err
	}

	distinctSQL := fmt.Sprintf(
		"SELECT COUNT(1) FROM (SELECT value FROM (%s) values_view%s GROUP BY value) grouped_values",
		sourceSQL,
		whereSQL,
	)
	var distinctCount int64
	if err := s.db.QueryRowContext(ctx, distinctSQL, filterArgs...).Scan(&distinctCount); err != nil {
		return AnalyticsDistribution{}, err
	}

	distributionSQL := fmt.Sprintf(`
		SELECT value, COUNT(1) AS count
		FROM (%s) values_view
		%s
		GROUP BY value
		ORDER BY count DESC, value ASC
		LIMIT ?`, sourceSQL, whereSQL)
	args := append(append(make([]any, 0, len(filterArgs)+1), filterArgs...), limit)
	rows, err := s.db.QueryContext(ctx, distributionSQL, args...)
	if err != nil {
		return AnalyticsDistribution{}, err
	}
	defer rows.Close()

	out := AnalyticsDistribution{
		Field:         spec.Name,
		Filter:        filter,
		Limit:         limit,
		TotalRows:     totalRows,
		MatchedRows:   matchedRows,
		DistinctCount: distinctCount,
		Buckets:       make([]AnalyticsBucket, 0, limit),
	}

	for rows.Next() {
		var bucket AnalyticsBucket
		if err := rows.Scan(&bucket.Value, &bucket.Count); err != nil {
			return AnalyticsDistribution{}, err
		}
		if out.MatchedRows > 0 {
			bucket.Percentage = (float64(bucket.Count) / float64(out.MatchedRows)) * 100
		}
		out.Buckets = append(out.Buckets, bucket)
	}

	if err := rows.Err(); err != nil {
		return AnalyticsDistribution{}, err
	}
	return out, nil
}

func (s *Store) AnalyticsCount(ctx context.Context, field, value string) (AnalyticsCountResult, error) {
	spec, err := s.analyticsFieldSpec(ctx, field)
	if err != nil {
		return AnalyticsCountResult{}, err
	}

	query := fmt.Sprintf(
		"SELECT COUNT(1) FROM (SELECT %s AS value FROM records) values_view WHERE value = ?",
		spec.Expr,
	)

	var count int64
	if err := s.db.QueryRowContext(ctx, query, value).Scan(&count); err != nil {
		return AnalyticsCountResult{}, err
	}
	return AnalyticsCountResult{
		Field: spec.Name,
		Value: value,
		Count: count,
	}, nil
}

func (s *Store) analyticsFieldSpec(ctx context.Context, field string) (analyticsFieldSpec, error) {
	field = strings.ToLower(strings.TrimSpace(field))
	specs, err := s.analyticsFieldSpecs(ctx)
	if err != nil {
		return analyticsFieldSpec{}, err
	}
	for _, spec := range specs {
		if spec.Name == field {
			return spec, nil
		}
	}
	return analyticsFieldSpec{}, fmt.Errorf("unsupported field %q", field)
}

func (s *Store) analyticsFieldSpecs(ctx context.Context) ([]analyticsFieldSpec, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT name
		FROM pragma_table_info('records')
		WHERE name NOT IN ('row_num', 'file_offset', 'line_length')
		ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	specs := make([]analyticsFieldSpec, 0, 12)
	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, err
		}

		specs = append(specs, analyticsFieldSpec{
			Name:  name,
			Label: analyticsFieldLabel(name),
			Expr:  analyticsValueExpr(name),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return specs, nil
}

func analyticsFieldLabel(name string) string {
	if override, ok := analyticsLabelOverrides[name]; ok {
		return override
	}

	parts := strings.Split(name, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}
	return strings.Join(parts, " ")
}

func analyticsValueExpr(column string) string {
	quoted := analyticsQuoteIdent(column)
	if column == "is_active" || column == "is_deleted" {
		return fmt.Sprintf("CASE WHEN %s = 1 THEN 'true' ELSE 'false' END", quoted)
	}
	return fmt.Sprintf("COALESCE(NULLIF(TRIM(CAST(%s AS TEXT)), ''), '(empty)')", quoted)
}

func analyticsQuoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func anyToString(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return value
	case bool:
		if value {
			return "true"
		}
		return "false"
	case float64:
		return strconv.FormatFloat(value, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", value)
	}
}

func parseAnyBool(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case float64:
		return value != 0
	case string:
		return parseBool(value)
	default:
		return false
	}
}

func collectJSONIndexedFields(path string, value any, out *[]jsonIndexedField) {
	switch v := value.(type) {
	case map[string]any:
		for key, child := range v {
			nextPath := key
			if path != "" {
				nextPath = path + "." + key
			}
			collectJSONIndexedFields(nextPath, child, out)
		}
	case []any:
		for i, child := range v {
			nextPath := fmt.Sprintf("[%d]", i)
			if path != "" {
				nextPath = fmt.Sprintf("%s[%d]", path, i)
			}
			collectJSONIndexedFields(nextPath, child, out)
		}
	case string:
		appendJSONIndexedField(path, v, "string", out)
	case float64:
		appendJSONIndexedField(path, strconv.FormatFloat(v, 'f', -1, 64), "number", out)
	case bool:
		appendJSONIndexedField(path, strconv.FormatBool(v), "bool", out)
	case nil:
		appendJSONIndexedField(path, "null", "null", out)
	}
}

func appendJSONIndexedField(path, valueText, valueType string, out *[]jsonIndexedField) {
	if path == "" {
		path = "$"
	}
	*out = append(*out, jsonIndexedField{
		Path:      path,
		ValueText: valueText,
		ValueType: valueType,
	})
}

func indexParseWorkerCount() int {
	n := runtime.NumCPU()
	if n < 2 {
		return 2
	}
	if n > 12 {
		return 12
	}
	return n
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
