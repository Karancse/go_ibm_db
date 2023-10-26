// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package go_ibm_db

import (
	"database/sql/driver"
	"unsafe"

	"github.com/ibmdb/go_ibm_db/api"
)

type Conn struct {
	h  api.SQLHDBC
	tx *Tx
}

func (d *Driver) Open(dsn string) (driver.Conn, error) {
	var out api.SQLHANDLE
	ret := api.SQLAllocHandle(api.SQL_HANDLE_DBC, api.SQLHANDLE(d.h), &out)
	if IsError(ret) {
		return nil, NewError("SQLAllocHandle", d.h)
	}
	h := api.SQLHDBC(out)
	drv.Stats.updateHandleCount(api.SQL_HANDLE_DBC, 1)

	b := api.StringToUTF16(dsn)
	ret = api.SQLDriverConnect(h, 0,
		(*api.SQLWCHAR)(unsafe.Pointer(&b[0])), api.SQLSMALLINT(len(b)),
		nil, 0, nil, api.SQL_DRIVER_NOPROMPT)
	if IsError(ret) {
		defer releaseHandle(h)
		return nil, NewError("SQLDriverConnect", h)
	}
	return &Conn{h: h}, nil
}

func (c *Conn) Close() error {
	ret := api.SQLDisconnect(c.h)
	if IsError(ret) {
		return NewError("SQLDisconnect", c.h)
	}
	h := c.h
	c.h = api.SQLHDBC(api.SQL_NULL_HDBC)
	return releaseHandle(h)
}

//Query method executes the statement with out prepare if no args provided, and a driver.ErrSkip otherwise (handled by sql.go to execute usual preparedStmt)
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	if len(args) > 0 {
		// Not implemented for queries with parameters
		return nil, driver.ErrSkip
	}
	var out api.SQLHANDLE
	var os *ODBCStmt
	ret := api.SQLAllocHandle(api.SQL_HANDLE_STMT, api.SQLHANDLE(c.h), &out)
	if IsError(ret) {
		return nil, NewError("SQLAllocHandle", c.h)
	}
	h := api.SQLHSTMT(out)

	//	Edited Code  
	
	// number of rows to fetch using SQLFetchScroll
	nR := 1
	
	/*  Set the number of rows fetched during SQLFetchScroll */
	ret1 := api.SQLSetStmtAttr(h,api.SQL_ATTR_ROW_ARRAY_SIZE,
		(api.SQLPOINTER)(nR),api.SQL_IS_INTEGER)
	if IsError(ret1) {
		return nil, NewError("SQLSetStmtAttr", h)
	}

	/*  Set the cursor type to Dynamic cursor */
	ret1 = api.SQLSetStmtAttr(h,api.SQL_ATTR_CURSOR_TYPE,
		//(api.SQLPOINTER)(api.SQL_CURSOR_DYNAMIC),api.SQL_IS_INTEGER)
		api.SQL_CURSOR_DYNAMIC,api.SQL_IS_INTEGER)
	if IsError(ret1) {
		return nil, NewError("SQLSetStmtAttr", h)
	}

	/*  Number of rows fetched during SQLFetchScroll will be stored in numrowsfetchedptr */
	var numrowsfetchedptr uint64
	ret1 = api.SQLSetStmtAttr(h,api.SQL_ATTR_ROWS_FETCHED_PTR,
		(api.SQLPOINTER)(unsafe.Pointer(&numrowsfetchedptr)),api.SQL_IS_INTEGER)
		//(api.SQLPOINTER)(numrowsfetchedptr),api.SQL_IS_INTEGER)
	if IsError(ret1) {
		return nil, NewError("SQLSetStmtAttr", h)
	}

	/* Setting b1 as the row status pointer */
	var b1 = make([]byte,nR)
	ret1 = api.SQLSetStmtAttr(h,api.SQL_ATTR_ROW_STATUS_PTR,
		(api.SQLPOINTER)(unsafe.Pointer(&b1[0])),api.SQL_IS_INTEGER)
	if IsError(ret1) {
		return nil, NewError("SQLSetStmtAttr", h)
	}
	
	drv.Stats.updateHandleCount(api.SQL_HANDLE_STMT, 1)
	b := api.StringToUTF16(query)
	ret = api.SQLExecDirect(h,
		(*api.SQLWCHAR)(unsafe.Pointer(&b[0])), api.SQL_NTS)
	if IsError(ret) {
		defer releaseHandle(h)
		return nil, NewError("SQLExecDirectW", h)
	}
	ps, err := ExtractParameters(h)
	if err != nil {
		defer releaseHandle(h)
		return nil, err
	}
	os = &ODBCStmt{
		h:          h,
		Parameters: ps,
		usedByRows: true}
	err = os.BindColumns()
	if err != nil {
		return nil, err
	}
	if (true) {
		ret1 = api.SQLSetPos(h, 1, api.SQL_UPDATE, api.SQL_LOCK_NO_CHANGE);
		if IsError(ret1) {
			return nil, NewError("SQLSetPos", h)
		}
	}
	return &Rows{os: os}, nil
}
