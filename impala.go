package main

import (
	"golang.org/x/net/context"
	"fmt"
	"log"

	impala "github.com/bippio/go-impala"
)

func main() {
	// host := "Impala Load Balancer Host IP"
	host := "localhost"
	port := 21000

	opts := impala.DefaultOptions

	// enable LDAP authentication:
	// opts.UseLDAP = true
	// opts.Username = "LDAP Username"
	// opts.Password = "LDAP Password"

	// enable TLS
	// opts.UseTLS = true
	// opts.CACertPath = "cert.pem"

	con, err := impala.Connect(host, port, &opts)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// get all databases for the connection object
	query := fmt.Sprintf("SHOW DATABASES")
	rows, err := con.Query(ctx, query)
	if err != nil {
		log.Fatal("error in retriving databases: ", err)
	}

	databases := make([]string, 0) // databases will contain all the DBs to enumerate later
	for rows.Next(ctx) {
		row := make(map[string]interface{})
		err = rows.MapScan(row)
		if err != nil {
			log.Println(err)
			continue
		}
		if db, ok := row["name"].(string); ok {
			databases = append(databases, db)
		}
	}
	log.Print("List of Databases\n", databases)

	for _, d := range databases {
		q := "SHOW TABLES IN " + d

		results, err := con.Query(ctx, q)
		if err != nil {
			log.Printf("error in querying database %s: %s", d, err.Error())
			continue
		}

		tables := make([]string, 0) // databases will contain all the DBs to enumerate later
		for results.Next(ctx) {
			row := make(map[string]interface{})
			err = results.MapScan(row)
			if err != nil {
				log.Println(err)
				continue
			}
			if tab, ok := row["name"].(string); ok {
				tables = append(tables, tab)
			}
		}
		log.Printf("List of Tables in Database %s: %v\n", d, tables)
	}
        q2 := "select c_name from tpch_parquet.customer"

        rows, err = con.Query(ctx, q2)
        if err != nil {
                log.Fatal("error in query %s: %s", q2, err.Error())
        }

        results := make([]string, 0)
        for rows.Next(ctx) {
                row := make(map[string]interface{})
                err = rows.MapScan(row)
                if err != nil {
                        log.Println(err)
                        break
                }
                if tab, ok := row["c_name"].(string); ok {
                        results = append(results, tab)
                }
        }
        log.Printf("R: %v\n", results)
}
