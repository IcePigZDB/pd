// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// +build tools

package tools

import (
	_ "github.com/go-playground/overalls"
	_ "github.com/kevinburke/go-bindata/go-bindata"
	_ "github.com/mgechev/revive"
	_ "github.com/pingcap/failpoint/failpoint-ctl"
	_ "github.com/sasha-s/go-deadlock"
	_ "github.com/swaggo/swag/cmd/swag"
	_ "golang.org/x/tools/cmd/goimports"
	_ "honnef.co/go/tools/cmd/staticcheck"
)
