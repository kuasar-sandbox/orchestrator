//go:build fdb && cgo

package fdbdriver

/*
#cgo LDFLAGS: -lfdb_c
#define FDB_API_VERSION 730
#include <foundationdb/fdb_c.h>
*/
import "C"

func nativePredicate(predicate, code int) bool {
	return C.fdb_error_predicate(C.int(predicate), C.fdb_error_t(code)) != 0
}
