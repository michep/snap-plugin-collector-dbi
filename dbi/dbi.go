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

package dbi

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/dtype"
	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/parser"
	"github.com/intelsdi-x/snap-plugin-utilities/config"
	"github.com/intelsdi-x/snap/control/plugin"
	"github.com/intelsdi-x/snap/control/plugin/cpolicy"
	"github.com/intelsdi-x/snap/core"
)

const (
	// Name of plugin
	Name = "dbi"
	// Version of plugin
	Version = 4
	// Type of plugin
	Type = plugin.CollectorPluginType
)

// DbiPlugin holds information about the configuration database and defined queries
type DbiPlugin struct {
	databases   map[string]*dtype.Database
	queries     map[string]*dtype.Query
	initialized bool
}

// CollectMetrics returns values of desired metrics defined in mts
func (dbiPlg *DbiPlugin) CollectMetrics(mts []plugin.MetricType) ([]plugin.MetricType, error) {

	var err error
	metrics := []plugin.MetricType{}
	data := map[string]interface{}{}

	fmt.Printf("!!!DEBUG CollectMetrics() mts=%+v\n", mts)

	// initialization - done once
	if dbiPlg.initialized == false {
		// CollectMetrics(mts) is called only when mts has one item at least
		err = dbiPlg.setConfig(mts[0])
		if err != nil {
			// Cannot obtained sql settings
			return nil, err
		}
		err = openDBs(dbiPlg.databases)
		if err != nil {
			return nil, err
		}
		dbiPlg.initialized = true
	}

	// execute dbs queries and get output
	data, err = dbiPlg.executeQueries()
	if err != nil {
		return nil, err
	}

	for _, m := range mts {
		if value, ok := data[m.Namespace().String()]; ok {
			metric := plugin.MetricType{
				Namespace_: m.Namespace(),
				Data_:      value,
				Timestamp_: time.Now(),
				Tags_:      m.Tags(),
				Version_:   m.Version(),
			}
			metrics = append(metrics, metric)
		}
	}

	return metrics, nil
}

// GetConfigPolicy returns config policy
func (dbiPlg *DbiPlugin) GetConfigPolicy() (*cpolicy.ConfigPolicy, error) {
	c := cpolicy.New()
	return c, nil
}

// GetMetricTypes returns metrics types exposed by snap-plugin-collector-dbi
func (dbiPlg *DbiPlugin) GetMetricTypes(cfg plugin.ConfigType) ([]plugin.MetricType, error) {
	mts := []plugin.MetricType{}

	err := dbiPlg.setConfig(cfg)
	if err != nil {
		// cannot obtained sql settings from Global Config
		return nil, err
	}

	for _, db := range dbiPlg.databases { // cycle databases
		for _, queryName := range db.QrsToExec { // cycle database queries
			query := dbiPlg.queries[queryName]
			for _, result := range query.Results { // cycle query results
				namespace := core.NewNamespace(nsPrefix...)
				// namespace = namespace.AddStaticElement(dbname) //TODO: Do we really need database name as static element, if we can configure it in namespace?
				for _, ns := range result.Namespace { // cycle result namespaces
					switch ns.Source {
					case "string":
						namespace = namespace.AddStaticElement(ns.String)
					default:
						namespace = namespace.AddDynamicElement(ns.Name, ns.Description)
					}
				}
				mt := plugin.MetricType{
					Namespace_: namespace,
				}
				mts = append(mts, mt)
			}
		}
	}

	return mts, nil
}

// New returns snap-plugin-collector-dbi instance
func New() *DbiPlugin {
	dbiPlg := &DbiPlugin{databases: map[string]*dtype.Database{}, queries: map[string]*dtype.Query{}, initialized: false}

	return dbiPlg
}

// setConfig extracts config item from Global Config or Metric Config, parses its contents (mainly information
// about databases and queries) and assigned them to appriopriate DBiPlugin fields
func (dbiPlg *DbiPlugin) setConfig(cfg interface{}) error {
	setFile, err := config.GetConfigItem(cfg, "setfile")
	if err != nil {
		// cannot get config item
		return err
	}

	dbiPlg.databases, dbiPlg.queries, err = parser.GetDBItemsFromConfig(setFile.(string))
	if err != nil {
		// cannot parse sql config contents
		return err
	}

	return nil
}

// executeQueries executes all defined queries of each database and returns results as map to its values,
// where keys are equal to columns' names
func (dbiPlg *DbiPlugin) executeQueries() (map[string]interface{}, error) {
	data := map[string]interface{}{}
	//metrics := []plugin.MetricType{}

	//execute queries for each defined databases
	for dbName, db := range dbiPlg.databases {
		if !db.Active {
			//skip if db is not active (none established connection)
			fmt.Fprintf(os.Stderr, "Cannot execute queries for database %s, is inactive (connection was not established properly)\n", dbName)
			continue
		}

		// retrieve name from queries to be executed for this db
		for _, queryName := range db.QrsToExec {
			statement := dbiPlg.queries[queryName].Statement

			fmt.Printf("!!!DEBUG executeQueries() queryName=%+v\n", queryName)
			fmt.Printf("!!!DEBUG executeQueries() statement=%+v\n", statement)

			out, err := db.Executor.Query(queryName, statement)
			if err != nil {
				// log failing query and take the next one
				fmt.Fprintf(os.Stderr, "Cannot execute query %s for database %s", queryName, dbName)
				continue
			}

			fmt.Printf("!!!DEBUG executeQueries() out=%+v\n", out)
			fmt.Printf("!!!DEBUG executeQueries() dbiPlg.queries[queryName].Results=%+v\n", dbiPlg.queries[queryName].Results)

			for resName, res := range dbiPlg.queries[queryName].Results {
				instanceOk := false
				// to avoid inconsistency of columns names caused by capital letters (especially for postgresql driver)
				//instanceFrom := strings.ToLower(res.InstanceFrom)

				fmt.Printf("!!!DEBUG executeQueries() resName=%+v\n", resName)
				fmt.Printf("!!!DEBUG executeQueries() res=%+v\n", res)

				instanceFrom := strings.ToLower(res.Namespace[0].InstanceFrom)
				valueFrom := strings.ToLower(res.ValueFrom)

				if !isEmpty(instanceFrom) {
					if len(out[instanceFrom]) == len(out[valueFrom]) {
						instanceOk = true
					}
				}

				fmt.Printf("!!!DEBUG executeQueries() instanceOk=%+v\n", instanceOk)

				for index, value := range out[valueFrom] {
					instance := ""

					if instanceOk {
						instance = fmt.Sprintf("%v", fixDataType(out[instanceFrom][index]))
					}

					fmt.Printf("!!!DEBUG executeQueries() instance=%+v\n", instance)

					//TODO: !!!! create correct Namespace with dynamic elements
					key := createNamespace(dbName, resName, res.InstancePrefix, instance)

					fmt.Printf("!!!DEBUG executeQueries() key=%+v\n", key)

					if _, exist := data[key]; exist {
						return nil, fmt.Errorf("Namespace `%s` has to be unique, but is not", key)
					}

					fmt.Printf("!!!DEBUG executeQueries() value=%+v\n", value)
					fmt.Printf("!!!DEBUG executeQueries() fixDataType(value)=%+v\n", fixDataType(value))

					data[key] = fixDataType(value)
				}
			}
		} // end of range db_queries_to_execute
	} // end of range databases

	if len(data) == 0 {
		return nil, fmt.Errorf("No data obtained from defined queries")
	}

	fmt.Printf("!!!DEBUG executeQueries() data=%+v\n", data)
	return data, nil
}

// fixDataType converts `arg` to a string if its type is an array of bytes or time.Time, in other case there is no change
func fixDataType(arg interface{}) interface{} {
	var result interface{}

	switch arg.(type) {
	case []byte:
		result = string(arg.([]byte))

	case time.Time:
		// gob: type time.Time is not registered for gob interface, conversion to string
		result = arg.(time.Time).String()

	default:
		result = arg
	}

	return result
}
