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
	"time"

	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/dtype"
	"github.com/intelsdi-x/snap-plugin-collector-dbi/dbi/parser"
	"github.com/intelsdi-x/snap-plugin-lib-go/v1/plugin"
	"strings"
)

const (
	Name    = "dbi"
	Version = 4
)

// DbiPlugin holds information about the configuration database and defined queries
type DbiPlugin struct {
	database    *dtype.Database
	queries     map[string]*dtype.Query
	initialized bool
}

// CollectMetrics returns values of desired metrics defined in mts
func (dbiPlg *DbiPlugin) CollectMetrics(mts []plugin.Metric) ([]plugin.Metric, error) {

	var err error
	metrics := []plugin.Metric{}

	// initialization - done once
	if dbiPlg.initialized == false {
		err = dbiPlg.setQueriesConfig(mts[0].Config)
		if err != nil {
			// cannot obtained sql settings from Global Config
			return nil, err
		}
		err = dbiPlg.setDatabaseConfig(mts[0].Config)
		if err != nil {
			// Cannot obtained sql settings
			return nil, err
		}
		err = openDBs(dbiPlg.database)
		if err != nil {
			return nil, err
		}
		dbiPlg.initialized = true
	}

	dbiPlg.database.Executor.ClearCachedResults()

	offset := len(nsPrefix)
	for _, m := range mts {
		for _, queryName := range dbiPlg.database.QrsToExec {
			query := dbiPlg.queries[queryName]
			for _, res := range query.Results {
				if strings.Join(res.CoreNamespace.Strings(), "/") == strings.Join(m.Namespace.Strings(), "/") {
					rows, data, err := dbiPlg.database.Executor.Query(queryName, query.Statement)
					if err != nil {
						// log failing query and take the next one
						fmt.Fprintf(os.Stderr, "Cannot execute query %s for database %s", queryName, dbiPlg.database.DBName)
						continue
					}
					for r := 0; r < rows; r++ {
						nspace := copyNamespaceStructure(res.CoreNamespace)
						value := data[res.ValueFrom][r]
						if dynamic, dynIdx := nspace.IsDynamic(); dynamic {
							for _, idx := range dynIdx {
								restype := res.Namespace[idx-offset].Type
								if restype == "connectionname" {
									nspace[idx].Value = dbiPlg.database.Name
									continue
								}
								col := res.Namespace[idx-offset].InstanceFrom
								val := data[col][r]
								nspace[idx].Value = val.(string)
								if restype == "expand" {
									nspace[idx].Name = ""
									nspace[idx].Description = ""
								}
							}
						}
						metric := plugin.Metric{
							Namespace: nspace,
							Data:      value,
							Timestamp: time.Now(),
							Tags:      m.Tags,
							Version:   m.Version,
						}
						metrics = append(metrics, metric)
					}
				}
			}
		}
	}

	return metrics, nil
}

// GetConfigPolicy returns config policy
func (dbiPlg *DbiPlugin) GetConfigPolicy() (plugin.ConfigPolicy, error) {
	c := plugin.NewConfigPolicy()
	c.AddNewStringRule([]string{"intel", "dbi"}, "setfile", true)
	c.AddNewStringRule([]string{"intel", "dbi"}, "name", true)
	c.AddNewStringRule([]string{"intel", "dbi"}, "driver", true)
	c.AddNewStringRule([]string{"intel", "dbi"}, "host", true)
	c.AddNewStringRule([]string{"intel", "dbi"}, "port", false)
	c.AddNewStringRule([]string{"intel", "dbi"}, "username", false)
	c.AddNewStringRule([]string{"intel", "dbi"}, "password", false)
	c.AddNewStringRule([]string{"intel", "dbi"}, "dbname", false)
	c.AddNewStringRule([]string{"intel", "dbi"}, "dbqueries", false)
	return *c, nil
}

// GetMetricTypes returns metrics types exposed by snap-plugin-collector-dbi
func (dbiPlg *DbiPlugin) GetMetricTypes(cfg plugin.Config) ([]plugin.Metric, error) {
	mts := []plugin.Metric{}

	err := dbiPlg.setQueriesConfig(cfg)
	if err != nil {
		// cannot obtained sql settings from Global Config
		return nil, err
	}

	for _, query := range dbiPlg.queries { // cycle queries
		for _, result := range query.Results { // cycle query results
			mt := plugin.Metric{
				Namespace: result.CoreNamespace,
				Version:   Version,
			}
			mts = append(mts, mt)
		}
	}

	return mts, nil
}

// New returns snap-plugin-collector-dbi instance
func New() *DbiPlugin {
	dbiPlg := &DbiPlugin{queries: map[string]*dtype.Query{}, initialized: false}

	return dbiPlg
}

// setConfig extracts config item from Global Config or Metric Config, parses its contents (mainly information
// about databases and queries) and assigned them to appriopriate DBiPlugin fields
func (dbiPlg *DbiPlugin) setQueriesConfig(cfg plugin.Config) error {
	var err error

	setFile := cfg["setfile"]

	dbiPlg.queries, err = parser.GetQueriesFromConfig(setFile.(string))

	if err != nil {
		// cannot parse sql config contents
		return err
	}

	for queryName, query := range dbiPlg.queries { // cycle queries
		for resName, result := range query.Results { // cycle query results
			namespace := plugin.NewNamespace(nsPrefix...)
			for _, ns := range result.Namespace { // cycle result namespaces
				switch ns.Type {
				case "static":
					namespace = namespace.AddStaticElement(ns.String)
				default:
					namespace = namespace.AddDynamicElement(ns.Name, ns.Description)
				}
			}
			result.CoreNamespace = namespace
			dbiPlg.queries[queryName].Results[resName] = result
		}
	}

	return nil
}

func (dbiPlg *DbiPlugin) setDatabaseConfig(cfg plugin.Config) error {
	var err error

	dbiPlg.database, err = parser.GetDatabaseFromConfig(cfg)

	if err != nil {
		// cannot parse sql config contents
		return err
	}

	return nil
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

func copyNamespaceStructure(nspace plugin.Namespace) plugin.Namespace {
	ret := plugin.NewNamespace()
	for _, nse := range nspace {
		if nse.IsDynamic() {
			ret = ret.AddDynamicElement(nse.Name, nse.Description)
		} else {
			ret = ret.AddStaticElement(nse.Value)
		}
	}
	return ret
}
