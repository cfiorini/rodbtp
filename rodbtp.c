/**
 *   Rodbtp - Ruby binding for odbtp (http://odbtp.sourceforge.net)
 *   Copyright (C) 2009  Claudio Fiorini <claudio@cfiorini.it>
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
**/

/* Any help and suggestions are always welcome */

#include "ruby.h"
#include "rubyio.h"
#include "odbtp.h"
#include "st.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

VALUE rb_cOdbtp;
VALUE rb_cResOdbtp;

void Init_rodbtp();

void rodb_mark(int* hCon)
{
	rb_gc_mark(*hCon);
}

void rodb_free(int* hCon)
{
	free(hCon);
}

VALUE rodb_alloc(VALUE klass)
{
        odbHANDLE *hCon = malloc(sizeof(odbHANDLE));

        if(!(hCon = (odbHANDLE *)odbAllocate(NULL))) {
                rb_raise(rb_eException, "error on odbAllocate");
                return Qnil;
        }

        return Data_Wrap_Struct(klass, rodb_mark, rodb_free, hCon);
}

VALUE rodb_connect(VALUE klass, VALUE host, VALUE port, VALUE conn)
{
	odbHANDLE *hCon;
        Data_Get_Struct(klass, odbHANDLE, hCon);

        if( !odbLogin((odbHANDLE)hCon, RSTRING(host)->ptr, FIX2INT(port), ODB_LOGIN_NORMAL, RSTRING(conn)->ptr))
        {
                odbFree((odbHANDLE)hCon );
                return Qfalse;
        }
	return Qtrue;
}

VALUE rodb_disconnect(VALUE klass)
{
	odbHANDLE *hCon;

        Data_Get_Struct(klass, odbHANDLE, hCon);

        if(!odbLogout((odbHANDLE)hCon, ODB_LOGIN_RESERVED)) {
                rb_raise( rb_eException, "Logout Error");
                odbFree((odbHANDLE)hCon );
                return Qnil;
        }

        return Qnil;
}


VALUE rodbres_new(odbHANDLE *ptr)
{
	return Data_Wrap_Struct(rb_cResOdbtp, 0, 0, ptr);
}

VALUE rodb_execquery(VALUE klass, VALUE query)
{
	odbHANDLE *hCon;
	odbHANDLE hQry;

        Data_Get_Struct(klass, odbHANDLE, hCon);

        if(!(hQry = odbAllocate((odbHANDLE)hCon))) {
                rb_raise(rb_eException, "error on allocating memory");
		return rodb_disconnect(klass);
        }
	if( !odbExecute( hQry, RSTRING(query)->ptr ) ) {
		rb_raise( rb_eException, "Execute Failed: %s\n", odbGetErrorText( hQry ) );
		return rodb_disconnect(klass);
	}
	return rodbres_new((odbHANDLE *)hQry);
}

VALUE rodbres_each(VALUE klass)
{
	odbHANDLE *hQry;

	int n, numCols = 0;
	VALUE tmp_ary;

	Data_Get_Struct(klass, odbHANDLE, hQry);

	numCols = odbGetTotalCols((odbHANDLE)hQry);
	
    	while( odbFetchRow((odbHANDLE)hQry ) && !odbNoData((odbHANDLE)hQry ) ) {
		tmp_ary = rb_ary_new2(numCols);
		for(n = 1; n <= numCols; n++) {
			rb_ary_push(tmp_ary, rb_tainted_str_new2(odbColData((odbHANDLE)hQry, n)));
		}	
		rb_yield(tmp_ary);
	}

	return klass;
}

VALUE rodbres_fields(VALUE klass)
{
	odbHANDLE *hQry;
	int n, numCols = 0;

	Data_Get_Struct(klass, odbHANDLE, hQry);

	numCols = odbGetTotalCols((odbHANDLE)hQry);

	VALUE hsh = rb_hash_new();
	for(n = 1; n <= numCols; n++) {
		rb_hash_aset(hsh,
		ID2SYM(rb_intern(odbColName( (odbHANDLE)hQry, n) )) ,
		INT2FIX(odbColDataType( (odbHANDLE)hQry, n) ) );
	}
	return hsh;
}

void Init_rodbtp()
{
        rb_cOdbtp = rb_define_class("Rodbtp", rb_cObject);
        rb_define_singleton_method(rb_cOdbtp, "new", rodb_alloc, 0);
	rb_define_method(rb_cOdbtp, "connect", rodb_connect, 3);
	rb_define_method(rb_cOdbtp, "query", rodb_execquery, 1);
        rb_define_method(rb_cOdbtp, "disconnect", rodb_disconnect, 0);

	rb_cResOdbtp = rb_define_class("RodbRes", rb_cObject);
	rb_include_module(rb_cResOdbtp, rb_mEnumerable);
	rb_define_alias(rb_cResOdbtp, "result", "entries");
	rb_define_method(rb_cResOdbtp, "each", rodbres_each, 0);
	rb_define_method(rb_cResOdbtp, "fields", rodbres_fields, 0);
}
