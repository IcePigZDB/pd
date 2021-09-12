// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

const (
	hotReadRegionsPrefix  = "pd/api/v1/hotspot/regions/read"
	hotWriteRegionsPrefix = "pd/api/v1/hotspot/regions/write"
	hotStoresPrefix       = "pd/api/v1/hotspot/stores"
	hotRegionsHistory     = "pd/api/v1/hotspot/regions/history"
)

// NewHotSpotCommand return a hot subcommand of rootCmd
func NewHotSpotCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "hot",
		Short: "show the hotspot status of the cluster",
	}
	cmd.AddCommand(NewHotWriteRegionCommand())
	cmd.AddCommand(NewHotReadRegionCommand())
	cmd.AddCommand(NewHotStoreCommand())
	cmd.AddCommand(NewHotRegionsHistoryCommand())
	return cmd
}

// NewHotWriteRegionCommand return a hot regions subcommand of hotSpotCmd
func NewHotWriteRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "write [<store id> ...]",
		Short: "show the hot write regions",
		Run:   showHotWriteRegionsCommandFunc,
	}
	return cmd
}

func showHotWriteRegionsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(cmd, hotWriteRegionsPrefix, args)
	if err != nil {
		cmd.Println(err)
		return
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get write hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotReadRegionCommand return a hot read regions subcommand of hotSpotCmd
func NewHotReadRegionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "read [<store id> ...]",
		Short: "show the hot read regions",
		Run:   showHotReadRegionsCommandFunc,
	}
	return cmd
}

func showHotReadRegionsCommandFunc(cmd *cobra.Command, args []string) {
	prefix, err := parseOptionalArgs(cmd, hotReadRegionsPrefix, args)
	if err != nil {
		cmd.Println(err)
		return
	}
	r, err := doRequest(cmd, prefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get read hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotStoreCommand return a hot stores subcommand of hotSpotCmd
func NewHotStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "store",
		Short: "show the hot stores",
		Run:   showHotStoresCommandFunc,
	}
	return cmd
}

func showHotStoresCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, hotStoresPrefix, http.MethodGet)
	if err != nil {
		cmd.Printf("Failed to get store hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

// NewHotRegionsHistoryCommand return a hot history regions subcommand of hotSpotCmd
func NewHotRegionsHistoryCommand() *cobra.Command {
	cmd := &cobra.Command{
		// TODO
		// Need a better description.
		Use:   "history <start_time> <end_time> [<key> <value>]",
		Short: "show the hot history regions",
		Run:   showHotRegionsHistoryCommandFunc,
	}
	return cmd
}

func showHotRegionsHistoryCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) < 2 || len(args)%2 != 0 {
		cmd.Println(cmd.UsageString())
	}
	input, err := parseHotRegionsHistoryArgs(args)
	if err != nil {
		cmd.Printf("Failed to get read hotspot: %s\n", err)
	}
	data, _ := json.Marshal(input)
	r, err := doRequest(cmd, hotRegionsHistory, http.MethodGet, WithBody("application/json", bytes.NewBuffer(data)))
	if err != nil {
		cmd.Printf("Failed to get read hotspot: %s\n", err)
		return
	}
	cmd.Println(r)
}

func parseOptionalArgs(cmd *cobra.Command, prefix string, args []string) (string, error) {
	argsLen := len(args)
	if argsLen > 0 {
		prefix += "?"
	}
	for i, arg := range args {
		if _, err := strconv.Atoi(arg); err != nil {
			return "", errors.Errorf("store id should be a number, but got %s", arg)
		}
		if i != argsLen {
			prefix = prefix + "store_id=" + arg + "&"
		} else {
			prefix = prefix + "store_id=" + arg
		}
	}
	return prefix, nil
}

func parseHotRegionsHistoryArgs(args []string) (map[string]interface{}, error) {
	startTime, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return nil, errors.Errorf("start_time should be a number,but got %s", args[0])
	}
	endTime, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, errors.Errorf("end_time should be a number,but got %s", args[1])
	}
	input := map[string]interface{}{
		"start_time": startTime,
		"end_time":   endTime,
	}
	stringToIntSlice := func(s string) ([]int64, error) {
		results := make([]int64, 0)
		args := strings.Split(s, ",")
		for _, arg := range args {
			result, err := strconv.ParseInt(arg, 10, 64)
			if err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		return results, nil
	}
	for index := 2; index < len(args); index += 2 {
		switch args[index] {
		case "hot_region_type":
			input["hot_region_type"] = []string{args[index+1]}
		case "region_ids":
			results, err := stringToIntSlice(args[index+1])
			if err != nil {
				return nil, errors.Errorf("region_ids should be a number slice,but got %s", args[index+1])
			}
			input["region_ids"] = results
		case "store_ids":
			results, err := stringToIntSlice(args[index+1])
			if err != nil {
				return nil, errors.Errorf("store_ids should be a number slice,but got %s", args[index+1])
			}
			input["store_ids"] = results
		case "peer_ids":
			results, err := stringToIntSlice(args[index+1])
			if err != nil {
				return nil, errors.Errorf("peer_ids should be a number slice,but got %s", args[index+1])
			}
			input["peer_ids"] = results
		case "is_leader":
			isLeader, err := strconv.ParseBool(args[index+1])
			if err != nil {
				return nil, errors.Errorf("is_leader should be a bool,but got %s", args[index+1])
			}
			input["is_leaders"] = []bool{isLeader}
		case "is_learner":
			isLearner, err := strconv.ParseBool(args[index+1])
			if err != nil {
				return nil, errors.Errorf("is_learners should be a bool,but got %s", args[index+1])
			}
			input["is_learners"] = []bool{isLearner}
		default:
			return nil, errors.Errorf("key should be one of hot_region_type,region_ids,store_ids,peer_ids,is_leaders,is_learners")
		}
	}
	return input, nil
}
