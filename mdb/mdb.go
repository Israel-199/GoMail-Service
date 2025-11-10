package mdb

type EmailEntry struct {
	Id          int
	Email       string
	ConfirmedAt int
	OptOut      int
}
