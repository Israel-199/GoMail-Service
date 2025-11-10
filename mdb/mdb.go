package mdb

import (
	"database/sql"
	"log"
	"time"
	"github.com/mattn/go-sqlite3"
)

type EmailEntry struct {
	Id          int64
	Email       string
	ConfirmedAt *time.Time
	OptOut      bool
}

func TryCreate(db *sql.DB){
	_,err:=db.Exec(`
	CREATE TABLE emails (
	id  INTEGER PRIMARY KEY,
	email  TEXT UNIQUE,
	confirmed_at  INTEGER,
	opt_out   INTEGER
	);
	`)
	if err != nil {
		if sqlError, ok :=err.(sqlite3.Error); {
			if sqlError.Code != 1{
				log.Fatal(sqlError)
			}
		} else {
			log.Fatal(err)
		}
	}
}

func emailEntryFromRow(row *sql.Rows)(*EmailEntry,error){
	var id int64
	var email string
	var confirmedAt int64
	var optOut bool

	err:= row.Scan(&id,&email,&confirmedAt,&optOut)

	if err != nil {
        log.Println(err)
		return nil,err
	}
	t := time.Unix(confirmedAt,0)
	return &EmailEntry{Id:id,Email: email,ConfirmedAt:&t,OptOut: optOut},nil
}

func CreateEmail(db *sql.DB, email string) error{
	_,err := db.Exec(`INSERT INTO
	emails(email,confirmed_at,opt_out)
	VALUE(?,0,false)
	`,email)
	if err!=nil{
		log.Println(err)
		return err
	}
	return nil
}

func GetEmail(db *sql.DB, email string)(*EmailEntry,error){
	row,err := db.Query(`
	SELECT id, email, confirmed_at, opt_out
	FROM emails
	WHERE emails = ?
	`,email)
	if err!=nil{
		log.Println(err)
		return nil,err
	}
	defer row.Close()

	for rows.Next(){
		return emailEntryFromRow(rows)
	}

	return nil,nil
}
func UpdateEmail(db *sql.DB, email string)(*EmailEntry,error){
	t := entry.ConfirmedAt.Unix()
	_,err := db.Exec(`
	emails(email, confirmed_at, opt_out)
		VALUE(?,?,?)
		ON CONFLICT(email) DO UPDATE SET
		confirmed_at=?
		opt_out=?
	`,entry.Email,t,entry.OptOut,t,entry.OptOut)
	if err!=nil{
		log.Println(err)
		return nil,err
	}

	return nil
}
