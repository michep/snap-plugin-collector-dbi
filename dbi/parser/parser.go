// +build linux

/*
http://www.apache.org/licenses/LICENSE-2.0.txt
Copyright 2016 Intel Corporation
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package parser

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/dtype"
	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/executor"
	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/parser/cfg"
)

// Parser holds maps to queries and databases
type Parser struct {
	qrs map[string]*dtype.Query
	db  *dtype.Database
}

// GetDBItemsFromConfig parses the contents of the file `fName` and returns maps to
// databases and queries instances which structurs are pre-defined in package dtype
func GetDBItemsFromConfig(fName string) (*dtype.Database, map[string]*dtype.Query, error) {

	var sqlCnf cfg.SQLConfig

	if strings.ContainsAny(fName, "$") {
		// filename contains environment variable, expand it
		fName = expandFileName(fName)
	}

	data, err := ioutil.ReadFile(fName)
	if err != nil {
		return nil, nil, err
	}

	if len(data) == 0 {
		return nil, nil, fmt.Errorf("SQL settings file `%v` is empty", fName)
	}

	err = json.Unmarshal(data, &sqlCnf)

	//fmt.Printf("!!!DEBUG GetDBItemsFromConfig() sqlCnf=%+v\n", sqlCnf)

	if err != nil {
		return nil, nil, fmt.Errorf("Invalid structure of file `%v` to be unmarshalled", fName)
	}

	//fmt.Printf("!!!DEBUG GetDBItemsFromConfig() sqlCnf=%+v\n", sqlCnf)

	p := &Parser{
		qrs: map[string]*dtype.Query{},
		//db: map[string]*dtype.Database{},
	}

	//fmt.Printf("!!!DEBUG GetDBItemsFromConfig() p1=%+v\n", p)

	for _, query := range sqlCnf.Queries {
		err := p.addQuery(query)
		if err != nil {
			return nil, nil, err
		}

	}

	//fmt.Printf("!!!DEBUG GetDBItemsFromConfig() p2=%+v\n", p)

	err = p.addDatabase(sqlCnf.Database)
	if err != nil {
		return nil, nil, err
	}

	//fmt.Printf("!!!DEBUG GetDBItemsFromConfig() db=%+v\n", p.db)
	//fmt.Printf("!!!DEBUG GetDBItemsFromConfig() qrs=%+v\n", p.qrs)

	return p.db, p.qrs, nil
}

// addDatabase adds database instance to databases
func (p *Parser) addDatabase(dt cfg.DatabasesType) error {

	if len(strings.TrimSpace(dt.Name)) == 0 {
		return fmt.Errorf("Database name is empty")
	}

	//if _, exist := p.dbs[dt.Name]; exist {
	//	return fmt.Errorf("Database name `%+s` is not unique", dt.Name)
	//}

	//getting info about which queries are to be executed
	execQrs := []string{}
	for _, q := range dt.QueryToExecute {
		execQrs = append(execQrs, q.QueryName)
	}

	// adding database to databases map
	p.db = &dtype.Database{
		Driver:    dt.Driver,
		Host:      dt.DriverOption.Host,
		Port:      dt.DriverOption.Port,
		Username:  dt.DriverOption.Username,
		Password:  dt.DriverOption.Password,
		DBName:    dt.DriverOption.DbName,
		SelectDB:  dt.SelectDb,
		Active:    false,
		QrsToExec: execQrs,
		Executor:  executor.NewExecutor(),
	}

	return nil
}

// addQuery adds query instance to queries
func (p *Parser) addQuery(qt cfg.QueryType) error {

	if len(strings.TrimSpace(qt.Name)) == 0 {
		return fmt.Errorf("Query name is empty")
	}

	if _, exist := p.qrs[qt.Name]; exist {
		return fmt.Errorf("Query name `%+s` is not unique", qt.Name)
	}

	results := map[string]dtype.Result{}

	for _, r := range qt.Results {

		if _, exist := results[r.ResultName]; exist {
			return fmt.Errorf("Query `%+s` has Result `%+s` which name is not unique", qt.Name, r.ResultName)
		}

		namesp := []dtype.NamespaceT{}
		el := dtype.NamespaceT{}
		for _, ns := range r.Namespace {
			el = dtype.NamespaceT{
				Type:         ns.Type,
				String:       ns.String,
				Name:         ns.Name,
				Description:  ns.Description,
				InstanceFrom: ns.InstanceFrom,
			}
			namesp = append(namesp, el)
		}

		// add result to the map `results`
		results[r.ResultName] = dtype.Result{
			Namespace: namesp,
			ValueFrom: r.ValueFrom,
		}

	} // end of range q.Results

	// adding query to queries map
	p.qrs[qt.Name] = &dtype.Query{
		Statement: qt.Statement,
		Results:   results,
	}
	return nil
}

// expandFileName replaces name of environment variable with its value and returns expanded filename
func expandFileName(fName string) string {

	// split namespace to get its components
	fNameCmps := strings.Split(fName, "/")

	for i, fNameCmp := range fNameCmps {
		if strings.Contains(fNameCmp, "$") {
			envName := strings.TrimPrefix(fNameCmp, "$")
			if envValue := os.Getenv(envName); envValue != "" {
				// replace name of environment variable with its value
				fNameCmps[i] = envValue
			}
		}
	}
	return strings.Join(fNameCmps, "/")
}
