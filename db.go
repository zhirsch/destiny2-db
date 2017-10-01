package db

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"reflect"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	apiclient "github.com/zhirsch/destiny2-api/client"
	"github.com/go-openapi/runtime"
)

// A DB is a Destiny2 manifest database that provides static information about objects in the Destiny universe.
type DB struct {
	db *sql.DB
}

// Open opens the manifest database.
func Open(client *apiclient.BungieNet, auth runtime.ClientAuthInfoWriter) (*DB, error) {
	// Fetch the current manifest database's name.
	resp, err := client.Destiny2.Destiny2GetDestinyManifest(nil, auth)
	if err != nil {
		return nil, err
	}

	// Download the manifest database if it doesn't already exist.
	filename := path.Base(resp.Payload.Response.MobileWorldContentPaths["en"])
	if _, err := os.Stat(filename); err != nil {
		url := fmt.Sprintf("https://www.bungie.net%s", resp.Payload.Response.MobileWorldContentPaths["en"])
		log.Printf("Downloading the manifest database from %v", url)
		if err := download(url, filename); err != nil {
			return nil, err
		}
	}

	// Open the manifest database.
	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// Get returns the specified entries from a database table.
func (db *DB) Get(table string, hash uint32, t interface{}) (interface{}, error) {
	row := db.db.QueryRow(fmt.Sprintf("SELECT json FROM %s WHERE id=?", table), int32(hash))
	var encoded string
	if err := row.Scan(&encoded); err != nil {
		return nil, err
	}
	value := reflect.New(reflect.Indirect(reflect.ValueOf(t)).Type())
	if err := json.NewDecoder(strings.NewReader(encoded)).Decode(value.Interface()); err != nil {
		return nil, err
	}
	return value.Interface(), nil
}

// GetAll returns the entries in the specified database table.
func (db *DB) GetAll(table string, t interface{}) (interface{}, error) {
	rows, err := db.db.Query(fmt.Sprintf("SELECT json FROM %v", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	values := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(t)), 0, 0)
	for rows.Next() {
		var encoded string
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		value := reflect.New(reflect.Indirect(reflect.ValueOf(t)).Type())
		if err := json.NewDecoder(strings.NewReader(encoded)).Decode(value.Interface()); err != nil {
			return nil, err
		}
		values = reflect.Append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return values.Interface(), nil
}

func download(url, filename string) error {
	// Download the database.
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Open the downloaded bytes as a zip file.
	reader, err := zip.NewReader(bytes.NewReader(b), resp.ContentLength)
	if err != nil {
		return err
	}
	if len(reader.File) != 1 {
		return errors.New("expected one file in database archive")
	}
	f, err := reader.File[0].Open()
	if err != nil {
		return err
	}
	defer f.Close()

	// Open the output file.
	out, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer out.Close()

	// Copy the decompressed database to the output file.
	if _, err := io.Copy(out, f); err != nil {
		return err
	}

	return nil
}
