// Copyright 2012 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package go_ibm_db

import (
	"database/sql/driver"
	"unsafe"
	"fmt"
	"github.com/Karancse/go_ibm_db_fork/api"
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
func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {	if len(args) > 0 {
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

	//	Edited Code Part 1  

	nR := 1  // nR = maximum number of rows to fetch using SQLFetchScroll
	var numrowsfetched uint32  
	var b1 = make([]byte,nR)
	if (true) {

		/*  Sets the maximum number of rows fetched during SQLFetchScroll.
			It is working. But (*rows r).Scan() is not working when nR > 1 */
		ret = api.SQLSetStmtAttr(h,api.SQL_ATTR_ROW_ARRAY_SIZE,
			(api.SQLPOINTER)(nR),api.SQL_IS_INTEGER)
		if IsError(ret) {
			return nil, NewError("SQLSetStmtAttr", h)
		}

		/* Sets the cursor type to dynamic.
			But it is not working */
		ret = api.SQLSetStmtAttr(h,api.SQL_ATTR_CURSOR_TYPE,
			api.SQL_CURSOR_DYNAMIC,0)
		if IsError(ret) {
			return nil, NewError("SQLSetStmtAttr", h)
		}
		
		/* number of rows fetched during SQLFetchScroll will be stored at numrowsfetched. */
		//var numrowsfetched uint32
		ret = api.SQLSetStmtAttr(h,api.SQL_ATTR_ROWS_FETCHED_PTR,
			(api.SQLPOINTER)(unsafe.Pointer(&numrowsfetched)),api.SQL_IS_INTEGER)
		if IsError(ret) {
			return nil, NewError("SQLSetStmtAttr", h)
		}

		/* the status of the rows fetched during SQLFetchScroll are stored at b1.  */
		//var b1 = make([]byte,nR)
		ret = api.SQLSetStmtAttr(h,api.SQL_ATTR_ROW_STATUS_PTR,
			(api.SQLPOINTER)(unsafe.Pointer(&b1[0])),api.SQL_IS_INTEGER)
		if IsError(ret) {
			return nil, NewError("SQLSetStmtAttr", h)
		}

		/* SQLGetStmtAttr stores output in buf2. */ 
		buf2 := make([]byte,40)
		fmt.Println("buf2 before get = ",buf2)
		ret = api.SQLGetStmtAttr(h,api.SQL_ATTR_ROWS_FETCHED_PTR,
			buf2,40)
			//(api.SQLPOINTER)(unsafe.Pointer(&buf2[0])),40)
		fmt.Println("buf2 after  get = ",buf2)
		if IsError(ret) {
			return nil, NewError("SQLGetStmtAttr", h)
		}
	}
	// Edited Code Part 1 Ends
	
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

	// Edited Code Part 2

	if (true) {
		ret = api.SQLFetch(h)
		if ret == api.SQL_NO_DATA {
			return nil, NewError("SQLFetch", h)
		}
		if IsError(ret) {
			return nil, NewError("SQLFetch", h)
		}
	} else {
		/* SQLFetchScroll works only for nR or SQL_ATTR_ROW_ARRAY_SIZE = 1.
			Otherwise not working at (*rows r).Scan() */
		ret = api.SQLFetchScroll(h,api.SQL_FETCH_FIRST,0)
		if ret == api.SQL_NO_DATA {
			return nil, NewError("SQLFetchScroll", h)
		}
		if IsError(ret) {
			return nil, NewError("SQLFetchScroll", h)
		}
	}

	fmt.Println("(api.SQLPOINTER)(unsafe.Pointer(&numrowsfetched)) = ",(api.SQLPOINTER)(unsafe.Pointer(&numrowsfetched)))
	fmt.Println("numrowsfetched = ",numrowsfetched)
	fmt.Println("b1 = ",b1)
	v, err := os.Cols[0].Value(os.h, 0)
	if err != nil {
		return nil, err
	}
	fmt.Println("os.Cols[0].Value(os.h, 0) = ",v)

	if (false) {
		/* SQLSetPos is updating for row 1. 
			But it is not working because cursor type is not set to dynamic */
		ret = api.SQLSetPos(h, 1, api.SQL_UPDATE, api.SQL_LOCK_NO_CHANGE);
		if IsError(ret) {
			return nil, NewError("SQLSetPos", h)
		}
	}

	// Edited Code Part 2 Ends

	return &Rows{os: os}, nil
}
