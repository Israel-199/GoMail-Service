package mdb

import (
	"database/sql"
	"log"
	"time"
)

// EmailEntry represents a record in the "emails" table.
type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

// TryCreate creates the "emails" table if it doesn't exist yet.
func TryCreate(db *sql.DB) {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS emails (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		email TEXT UNIQUE NOT NULL,
		confirmed_at INTEGER DEFAULT 0,
		opt_out INTEGER DEFAULT 0
	);
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

// Converts one row from SQL into an EmailEntry struct
func emailEntryFromRow(row *sql.Rows) (*EmailEntry, error) {
	var id int64
	var email string
	var confirmedAt int64
	var optOutInt int

	if err := row.Scan(&id, &email, &confirmedAt, &optOutInt); err != nil {
		return nil, err
	}

	var confirmedAtTime *time.Time
	if confirmedAt > 0 {
		t := time.Unix(confirmedAt, 0)
		confirmedAtTime = &t
	}

	optOut := optOutInt != 0
	return &EmailEntry{Id: id, Email: email, ConfirmedAt: confirmedAtTime, OptOut: optOut}, nil
}

// CreateEmail inserts a new email into the database
func CreateEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
	INSERT INTO emails (email, confirmed_at, opt_out)
	VALUES (?, 0, 0)
	ON CONFLICT(email) DO NOTHING;
	`, email)
	if err != nil {
		log.Println("CreateEmail error:", err)
	}
	return err
}

// GetEmail retrieves a single email entry from the database
func GetEmail(db *sql.DB, email string) (*EmailEntry, error) {
	rows, err := db.Query(`
	SELECT id, email, confirmed_at, opt_out
	FROM emails
	WHERE email = ?
	`, email)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		return emailEntryFromRow(rows)
	}
	return nil, nil
}

// UpdateEmail updates an existing email entry (confirmation or opt-out)
func UpdateEmail(db *sql.DB, entry *EmailEntry) error {
	var confirmedAtUnix int64
	if entry.ConfirmedAt != nil {
		confirmedAtUnix = entry.ConfirmedAt.Unix()
	}
	_, err := db.Exec(`
	UPDATE emails
	SET confirmed_at = ?, opt_out = ?
	WHERE email = ?;
	`, confirmedAtUnix, entry.OptOut, entry.Email)
	if err != nil {
		log.Println("UpdateEmail error:", err)
	}
	return err
}

// DeleteEmail marks an email as opted out
func DeleteEmail(db *sql.DB, email string) error {
	_, err := db.Exec(`
	UPDATE emails
	SET opt_out = 1
	WHERE email = ?;
	`, email)
	if err != nil {
		log.Println("DeleteEmail error:", err)
	}
	return err
}

// GetEmailBatchQueryParams holds pagination parameters
type GetEmailBatchQueryParams struct {
	Page  int
	Count int
}

// GetEmailBatch retrieves multiple non-opted-out emails with pagination
func GetEmailBatch(db *sql.DB, params GetEmailBatchQueryParams) ([]EmailEntry, error) {
	rows, err := db.Query(`
	SELECT id, email, confirmed_at, opt_out
	FROM emails
	WHERE opt_out = 0
	ORDER BY id ASC
	LIMIT ? OFFSET ?;
	`, params.Count, (params.Page-1)*params.Count)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	emails := make([]EmailEntry, 0, params.Count)
	for rows.Next() {
		entry, err := emailEntryFromRow(rows)
		if err != nil {
			return nil, err
		}
		emails = append(emails, *entry)
	}

	return emails, nil
}
