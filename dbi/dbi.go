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
	// Name of plugin
	Name = "dbi"
	// Version of plugin
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

	//fmt.Printf("!!!DEBUG CollectMetrics() mts=%+v\n", mts)

	// initialization - done once
	if dbiPlg.initialized == false {
		// CollectMetrics(mts) is called only when mts has one item at least
		err = dbiPlg.setConfig(mts[0].Config)
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

	// execute dbs queries and get output
	//data, err = dbiPlg.executeQueries()
	//if err != nil {
	//	return nil, err
	//}

	dbiPlg.database.Executor.ClearCachedResults()

	offset := len(nsPrefix)
	for _, m := range mts {
		//fmt.Printf("!!!DEBUG CollectMetrics() m.Namespace().String()=%+v\n", m.Namespace().String())
		for _, queryName := range dbiPlg.database.QrsToExec {
			query := dbiPlg.queries[queryName]
			for _, res := range query.Results {
				//fmt.Printf("!!!DEBUG CollectMetrics() res.CoreNamespace.String()=%+v\n", res.CoreNamespace.String())
				if strings.Join(res.CoreNamespace.Strings(), "/") == strings.Join(m.Namespace.Strings(), "/") {
					rows, data, err := dbiPlg.database.Executor.Query(queryName, query.Statement)
					if err != nil {
						// log failing query and take the next one
						fmt.Fprintf(os.Stderr, "Cannot execute query %s for database %s", queryName, dbiPlg.database.DBName)
						continue
					}
					//fmt.Printf("!!!DEBUG CollectMetrics() rows=%+v\n", rows)
					//fmt.Printf("!!!DEBUG CollectMetrics() data=%+v\n", data)
					for r := 0; r < rows; r++ {
						//fmt.Printf("!!!DEBUG CollectMetrics() res.CoreNamespace=%+v\n", res.CoreNamespace)
						nspace := copyNamespaceStructure(res.CoreNamespace)
						//fmt.Printf("!!!DEBUG CollectMetrics() nspace1=%+v\n", nspace)
						value := data[res.ValueFrom][r]
						if dynamic, dynIdx := nspace.IsDynamic(); dynamic {
							for _, idx := range dynIdx {
								col := res.Namespace[idx-offset].InstanceFrom
								restype := res.Namespace[idx-offset].Type
								val := data[col][r]
								nspace[idx].Value = val.(string)
								if restype == "expand" {
									nspace[idx].Name = ""
									nspace[idx].Description = ""
								}
							}
						}
						//fmt.Printf("!!!DEBUG CollectMetrics() nspace2=%+v\n", nspace)
						//fmt.Printf("!!!DEBUG CollectMetrics() value=%+v\n", value)
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

	//fmt.Printf("!!!DEBUG CollectMetrics() metrics=%+v\n", metrics)
	return metrics, nil
}

// GetConfigPolicy returns config policy
func (dbiPlg *DbiPlugin) GetConfigPolicy() (plugin.ConfigPolicy, error) {
	c := plugin.NewConfigPolicy()
	return *c, nil
}

// GetMetricTypes returns metrics types exposed by snap-plugin-collector-dbi
func (dbiPlg *DbiPlugin) GetMetricTypes(cfg plugin.Config) ([]plugin.Metric, error) {
	mts := []plugin.Metric{}

	err := dbiPlg.setConfig(cfg)
	if err != nil {
		// cannot obtained sql settings from Global Config
		return nil, err
	}

	for _, queryName := range dbiPlg.database.QrsToExec { // cycle database queries
		query := dbiPlg.queries[queryName]
		//fmt.Printf("!!!DEBUG GetMetricTypes() queryName=%+v\n", queryName)
		for _, result := range query.Results { // cycle query results
			//fmt.Printf("!!!DEBUG GetMetricTypes() resName=%+v\n", resName)
			//fmt.Printf("!!!DEBUG GetMetricTypes() result.CoreNamespace=%+v\n", result.CoreNamespace)
			mt := plugin.Metric{
				Namespace: result.CoreNamespace,
				Version: Version,
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
func (dbiPlg *DbiPlugin) setConfig(cfg interface{}) error {
	var err error
	fmt.Printf("!!!DEBUG setConfig() before config.GetConfigItem\n")
	fmt.Printf("!!!DEBUG setConfig() cfg type %T\n", cfg)
	fmt.Printf("!!!DEBUG setConfig() cfg %+v\n", cfg)

	setFile := cfg.(plugin.Config)["setfile"]

	fmt.Printf("!!!DEBUG setConfig() after config.GetConfigItem\n")

	//fmt.Printf("!!!DEBUG setConfig() setFile=%+v\n", setFile)
	dbiPlg.database, dbiPlg.queries, err = parser.GetDBItemsFromConfig(setFile.(string))
	//fmt.Printf("!!!DEBUG setConfig() dbiPlg.databases=%+v\n", dbiPlg.database)
	//fmt.Printf("!!!DEBUG setConfig() dbiPlg.queries=%+v\n", dbiPlg.queries)

	if err != nil {
		// cannot parse sql config contents
		return err
	}

	for _, queryName := range dbiPlg.database.QrsToExec { // cycle database queries
		//fmt.Printf("!!!DEBUG setConfig() queryName=%+v\n", queryName)
		query := dbiPlg.queries[queryName]
		for resName, result := range query.Results { // cycle query results
			//fmt.Printf("!!!DEBUG setConfig() resName=%+v\n", resName)
			namespace := plugin.NewNamespace(nsPrefix...)
			for _, ns := range result.Namespace { // cycle result namespaces
				//fmt.Printf("!!!DEBUG setConfig() ns=%+v\n", ns)
				switch ns.Type {
				case "static":
					namespace = namespace.AddStaticElement(ns.String)
				default:
					namespace = namespace.AddDynamicElement(ns.Name, ns.Description)
				}
			}
			//fmt.Printf("!!!DEBUG setConfig() namespace=%+v\n", namespace)
			result.CoreNamespace = namespace
			dbiPlg.queries[queryName].Results[resName] = result
			//fmt.Printf("!!!DEBUG setConfig() FULLPATH=%+v\n", dbiPlg.queries[queryName].Results[resName].CoreNamespace.String())
		}
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
		//fmt.Printf("!!!DEBUG copyNamespaceStructure() nse=%+v\n", nse)
		if nse.IsDynamic() {
			ret = ret.AddDynamicElement(nse.Name, nse.Description)
		} else {
			ret = ret.AddStaticElement(nse.Value)
		}
	}
	//fmt.Printf("!!!DEBUG copyNamespaceStructure() ret=%+v\n", ret)
	return ret
}
