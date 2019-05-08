
#ifndef	_DATA_H_
#define	_DATA_H_

typedef  int SQLSTRLEN;

#define ASSERT_DEBUG(flag)  do {assert(flag);\
                                } while(0)

#define SQLOP_INSERT  0 
#define SQLOP_DELETE  1 
#define SQLOP_UPDATE  2 
#define SQLOP_SELECT  3 
#define SQLOP_CREATE  4
#define SQLOP_DROPTB  5 
#define SQLOP_TRABGN  6 
#define SQLOP_TRACOM  7 
#define SQLOP_TRARBK  8 
#define SQLOP_ALTER  9
#define SQLOP_COPYTB 10 
#define SQLOP_ATTACH  11 

typedef struct
{
    SBYTE sName;
    const char *cName;
    int sColNum;
}SQL_TABLE_S;

typedef struct
{
    SBYTE sName;
    const char *url;
}SQL_DATABASE_S;

typedef struct
{
    SBYTE sName;
    int DLT698Type;
    const char *cName;
    const char *type;
    const char *format;
}SQL_COL_S;

typedef struct
{
    SBYTE sName;
    const char *cName;
}SQLDATA_TABLE_S;

enum{DB_VAR_ESAM,DB_END,};
enum{VAR_ESAM_MAINTB,};

typedef struct
{
    int idx;
    const char *oi;
}OI_ENUM_S;

typedef struct
{
    SBYTE dbname;
    SBYTE cName;
    BOOL    (*pInit)();
}TBINIT_PFUNC;

#endif

