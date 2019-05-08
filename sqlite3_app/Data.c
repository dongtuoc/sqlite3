
#ifndef _sqlite3_
#define _sqlite3_

MUTEX DB_VAR_ESAM_CTRL;

const SQL_DATABASE_S SQL_DB[]=
{
    {DB_VAR_ESAM,		PATHNAME_SQLVAR"/esam.db3"},
};

const SQL_TABLE_S DBVAR_ESAM_TABLE[]={
    {VAR_ESAM_MAINTB,			"esam",			ESAM_COL_END},
}; 

const SQL_COL_S TB_ColSet_ESAM[] = {
    {ESAM_COL_OI,data_long_unsigned,"OI","varchar(8)","\"%s\""},
    {ESAM_COL_TEXT,data_structure,"DataSet","varchar(512)","\"%s\""},
};

const OI_ENUM_S TB_RowSet_ESAM[] = {
    {ESAM_ROW_F100,"OIF100"},
    {ESAM_ROW_F101,"OIF101"},
};

#ifndef _SQL_STATIC_
#define _SQL_STATIC_
/* must not change the following STATIC code */
static int _RowID_Num_ = 0;
static int _Name_Num_ = 0;
static int _MeasurePoint_Num_ = 0;

static BOOL SQL_MutexUnLock_(MUTEX *pMutex)
{
    assert(pMutex);
    return OsPackThreadMutexUnlock(pMutex);
}

static BOOL SQL_MutexLock_(MUTEX *pMutex)
{	
    assert(pMutex);
    return OsPackThreadMutexLock(pMutex,500);
}

/* UnLock when dbhandle is not a NULL */
static int SQL_Close_(SBYTE dbname,sqlite3 *dbhandle)
{
    int ret = sqlite3_close(dbhandle);
    BOOL res = FALSE;

    if (!dbhandle) return ret;

    switch(dbname)
    {
        case DB_VAR_ESAM:   res = SQL_MutexUnLock_(&DB_VAR_ESAM_CTRL);break;
        default:	break;
    }

    if (!res)
        return SQLITE_ERROR;	
    return ret;
}

/* first Lock, then open database */
static sqlite3 *SQL_Connect_(SBYTE dbname)
{
    sqlite3 *dbhandle 	= NULL;
    MUTEX *pMutex 		= NULL;
    SBYTE ret 			= 0; 

    switch(dbname)
    {
        case DB_VAR_ESAM:   pMutex = &DB_VAR_ESAM_CTRL;break;
        default:    return NULL;
    }

    if(!SQL_MutexLock_(pMutex))
        return NULL;

    ret = sqlite3_open(SQL_DB[dbname].url,&dbhandle);
    if ((ret != SQLITE_OK) || (dbhandle == NULL))
    {
        SQL_MutexUnLock_(pMutex);
        return NULL;
    }
    return dbhandle;
}

static const SQL_TABLE_S *SQL_GetTable_(SBYTE dbname,SBYTE sName)
{
    const SQL_TABLE_S *ptable = NULL;
    SBYTE len = 0;

    switch(dbname)
    {
    case DB_VAR_ESAM:
        ptable = DBVAR_ESAM_TABLE;
        len = sizeof(DBVAR_ESAM_TABLE)/sizeof(DBVAR_ESAM_TABLE[0]);
        break;
    default:
        return NULL;
    }

    if (sName >= len)
        return NULL;
    return ptable + sName;
}

static int SQL_GetTable_sColNum_(SBYTE dbname,SBYTE sName)
{
    const SQL_TABLE_S *pTable = SQL_GetTable_(dbname,sName);
    if (pTable) 
        return pTable->sColNum;
    return 0;
}

static const char *SQL_GetTable_cName_(SBYTE dbname,SBYTE sName)
{
    const SQL_TABLE_S *pTable = SQL_GetTable_(dbname,sName);
    if (pTable) 
        return pTable->cName;
    return NULL;
}

static const SQL_COL_S *SQL_GetTable_Col_(SBYTE dbname,SBYTE sName)
{
    const SQL_COL_S *pCol 	= NULL;
    SWORD selector 			=(SWORD)((dbname << 8) | sName);

    switch(selector)
    {
        case DB_VAR_ESAM << 8 | VAR_ESAM_MAINTB: 		
            pCol = TB_ColSet_ESAM;
            break;
        default:
            break;
    }
    return pCol;
}

static int callback_busy(void *ptr,int count)
{
    int time_out = *((int *)ptr);
    (void)usleep(time_out*1000);
    return 1;
}

static int SQL_Busy_Check_(sqlite3 *dbhandle,int ms)
{
    int ret = 0;
    if (ms > 0)
        ret = sqlite3_busy_handler(dbhandle,callback_busy,(void *)&ms);
    else
        ret = sqlite3_busy_handler(dbhandle,0,0);
    return ret;
}

static SBYTE SQL_Exec_(SBYTE dbname,SBYTE SQLOP,sqlite3 *dbhandle,const char *sql, int (*callback)(void*,int,char**,char**),void *arg)
{
    SBYTE ret = SQLITE_ERROR;
    int BusyVal = -1;
    char *errmsg = NULL;

    if (!dbhandle || !sql)
        return ret;

    BusyVal = SQL_Busy_Check_(dbhandle,100);
    if (BusyVal != SQLITE_OK)
        return ret;
	
    ret = sqlite3_exec(dbhandle,sql,callback,arg,&errmsg);
    if (ret != SQLITE_OK)
    {
        const char *err = sqlite3_errmsg(dbhandle);
        char *name = SQLTOOL_DebugGetDataBaseName(SQL_DB[dbname].url);
        char *op = NULL;
        switch(SQLOP)
        {
        case SQLOP_CREATE:op = "create";break;
        case SQLOP_INSERT:op = "insert";break;
        case SQLOP_DELETE:op = "delete";break;
        case SQLOP_UPDATE:op = "update";break;
        case SQLOP_SELECT:op = "select";break;
        case SQLOP_DROPTB:op = "droptb";break;
        case SQLOP_TRABGN:op = "trabgn";break;
        case SQLOP_TRACOM:op = "tracom";break;
        case SQLOP_TRARBK:op = "trarbk";break;
        case SQLOP_ALTER:op = "Alter";break;
        case SQLOP_COPYTB:op = "CopyTB";break;	
        case SQLOP_ATTACH:op = "Attach";break;		
        default:op = "default";break;
        }
    }

    sqlite3_free(errmsg);
    return ret;
}

/*
** ^If an sqlite3_exec() callback returns non-zero, the sqlite3_exec()
** routine returns SQLITE_ABORT without invoking the callback again and
** without running any subsequent SQL statements.
*/

static int SQL_CallbackGetRowID_(int *para,int ncolumn,char ** columnvalue,char *columnname[])
{
    SWORD len = 0;
    int RowID = 0;
    SWORD i = 0;
    char *value = columnvalue[0];

    if(!para) return 0;
    if (!value) return 0;

    len = strlen(value);
    while(len--)
    {
        RowID = RowID*10 + value[i] - '0';
        i++;
    }

    *para = RowID;
    return 0;
}

static int SQL_CallbackGetText_(SBYTE *para,int ncolumn,char ** columnvalue,char *columnname[])
{
    char *cText = columnvalue[0];

    if (!para) return 0;

    if (cText)
    {
        SQLTOOL_StrToSbyteArray(cText,(SBYTE *)(para));
    }
    return 0;
}

static int SQL_CallbackGetInt_(int *para,int ncolumn,char ** columnvalue,char *columnname[])
{
    SWORD len = 0;
    SWORD i = 0;
    char *cText = columnvalue[0];
    int a = 0;

    if (!para) return 0;

    len = strlen(cText);
    while(len--)
    {
        a = a*10 + cText[i] - '0';
        i++;
    }
    *para = a;
    return 0;
}

/*»ñÈ¡2×Ö½Ú³¤¶ÈÊý¾Ý*/
static int SQL_CallbackGetSmallInt_(unsigned short *para,int ncolumn,char ** columnvalue,char *columnname[])
{
    SWORD len = 0;
    SWORD i = 0;
    char *cText = columnvalue[0];
    SWORD a = 0;

    if (!para) return 0;

    len = strlen(cText);
    while(len--)
    {
        a = a*10 + cText[i] - '0';
        i++;
    }

    *para = a;
    return 0;
}

static int SQL_CallbackGetName_(char (*para)[64],int ncolumn,char ** columnvalue,char *columnname[])
{
    char *value = columnvalue[0];

    OsPackStrCopy_r(para[_Name_Num_],value);
    _Name_Num_++;
    return 0;
}
#endif

#ifndef _SQL_TOOL_
#define _SQL_TOOL_

#ifndef _SQLTOOL_TYPECONVERSION_
#define _SQLTOOL_TYPECONVERSION_

SQLSTRLEN SQLTOOL_GetRealSAlen(const SBYTE *in,SQLSTRLEN inlen)
{
    SQLSTRLEN i = 0;

    for(i=inlen-1; i>=0 && in[i]==0;i--);
    return (SQLSTRLEN)(i+1);
}

SQLSTRLEN SQLTOOL_GetRealSWlen(const SWORD *in,SQLSTRLEN SWNum)
{
    SQLSTRLEN i = 0;

    for(i=SWNum-1; i>=0 && in[i]==0;i--);
    return (SQLSTRLEN)(i+1);
}

void SQLTOOL_SbyteArrayToStr(char *out,const SBYTE *in,SWORD len)
{
    SQLSTRLEN RealLen = SQLTOOL_GetRealSAlen(in,len);
    SQLSTRLEN i = 0;
    char tmp[4] = {0};

    if (!RealLen || !in || !out) return;

    for(i = 0; i < RealLen; i++)
    {
        memset(tmp,0,sizeof(tmp));
        sprintf(tmp,"%02x",in[i]);
        strcat(out+i*2,tmp);
    }

    return;
}

void SQLTOOL_SwordArrayToStr(char *out,const SWORD *in,SWORD len)
{
    SQLSTRLEN RealLen = SQLTOOL_GetRealSWlen(in,len);
    SQLSTRLEN i = 0;
    char tmp[10] = {0};

    if (!in) return;
    if (!out) return;
    if (!RealLen) return;

    for(i = 0; i < RealLen; i++)
    {
        memset(tmp,0,sizeof(tmp));
        sprintf(tmp,"%04d",in[i]);
        strcat(out,tmp);
    }

    return;
}

void SQLTOOL_SwordHexArrayToStr(char *out,const SWORD *in,SWORD len)
{
    SQLSTRLEN RealLen = SQLTOOL_GetRealSWlen(in,len);
    SQLSTRLEN i = 0;
    char tmp[10] = {0};

    if (!in) return;
    if (!out) return;
    if (!RealLen) return;

    for(i = 0; i < RealLen; i++)
    {
        memset(tmp,0,sizeof(tmp));
        sprintf(tmp,"%04x",in[i]);
        strcat(out,tmp);
    }

    return;
}

void SQLTOOL_IntToStr(int in,char *out)
{
    if (!out) return;

    sprintf(out,"%d",in);
    return;
}

void SQLTOOL_AtimeToStr(ATIME in,char *out)
{
    SQLTOOL_IntToStr(in,out);
    return;
}

void SQLTOOL_TwoStrsCombin(char *out,const char *str1,const char *str2)
{
    if (!out ) return;
    if (!str1) return;
    if (!str2) return;

    strcat(out,str1);
    strcat(out,str2);
    return;
}

void SQLTOOL_StrIntCombin(char *out,const char *str,int val)
{
    char tmp[32]={0};

    if (!out) return;
    if (!str) return;

    SQLTOOL_IntToStr(val,tmp);
    SQLTOOL_TwoStrsCombin(out,str,tmp);
    return;
}

void SQLTOOL_IntStrCombin(char *out,int val,const char *str)
{
    char tmp[32]={0};

    if (!out) return;
    if (!str) return;

    SQLTOOL_IntToStr(val,tmp);
    SQLTOOL_TwoStrsCombin(out,tmp,str);
    return;
}

static char str_char(char *in)
{
    if (('0' <= *in) && (*in <= '9'))
    {
        return (*in - '0');
    }
    else if (('a' <= *in) && (*in <= 'f'))
    {
        return (*in - 'a' + 10);
    }
    else if (('A' <= *in) && (*in <= 'F'))
    {
        return (*in - 'A' + 10);
    }
    else
    {
        return 0;
    }
}

int SQLTOOL_StrToSbyteArray(const char *in, SBYTE *out)
{
    int len = OsPackStrLength_r(in);
    int ReturnLength = 0;
    SBYTE a,b;
    int i,j;	

    if (!in || !out || !len) return ReturnLength;

    if (len%2)
    {
        ReturnLength = len/2+1;

        for (i = 0,j = 0; i < ReturnLength; i++,j++)
        {
            if (i == 0)
            {
                a = 0;
                b = str_char(in+i);
            }
            else
            {
                a =  str_char(in+i*2-1);
                b = str_char(in+i*2);
            }
            *(out+ j) = (SBYTE)(((a<<4)&0xF0)|(b&0x0F));
        }	
    }
	else
	{
        ReturnLength = len/2;

        for(i = 0,j = 0; i < ReturnLength; i++,j++)
        {
            a = str_char(in+i*2);
            b = str_char(in+i*2+1);

            *(out+ j) = (SBYTE)(((a<<4)&0xF0)|(b&0x0F));
        }
    }
    return ReturnLength;
}

void SQLTOOL_StrToSword(const char *in, SWORD *out)
{
    SWORD i = 0;
    SWORD len = 0;
    SWORD tmp = 0;

    if (!in) return;
    if (!out) return;

    len = strlen(in);

    while(len--)
    {
        tmp = tmp*10 + in[i] - '0';
        i++;
    }
    *out = tmp;
    return;
}

int SQLTOOL_OADToInterger(OBJ_OAD *oad)
{
    int a = 0;

    a = (int)(oad->oi << 16 | oad->tagFeature << 8 | oad->idx);
    return a;
}

char *SQLTOOL_DebugGetDataBaseName(const char *url)
{
    int len = strlen(url);
    int i = 0;

    for (i=len-1; i>=0 && url[i]!= '/'; --i);
    return (char *)(url+i+1);
}
#endif


#ifndef _SQLTOOL_GETINFOMATION_
#define _SQLTOOL_GETINFOMATION_

const char *SQLTOOL_GetColcName(SBYTE dbname,SBYTE sName,SBYTE sColName)
{
    const SQL_COL_S *pCol = SQL_GetTable_Col_(dbname,sName);
    if (pCol)
        return pCol[sColName].cName;
    return NULL;
}

const char *SQLTOOL_GetColFormate(SBYTE dbname,SBYTE sName,SBYTE sColName)
{
    const SQL_COL_S *pCol = SQL_GetTable_Col_(dbname,sName);
    if (pCol)
        return pCol[sColName].format;
    return NULL;
}

const char *SQLTOOL_GetColType(SBYTE dbname,SBYTE sName,SBYTE sColName)
{
    const SQL_COL_S *pCol = SQL_GetTable_Col_(dbname,sName);
    if (pCol)
        return pCol[sColName].type;
    return NULL;
}

const char *SQLTOOL_GetTablecName(SBYTE dbname,SBYTE sName)
{
    return SQL_GetTable_cName_(dbname,sName);
}


int SQLTOOL_GetColNum(SBYTE dbname,SBYTE sName)
{
    return SQL_GetTable_sColNum_(dbname,sName);
}

void SQLTOOL_CreateCommentCombin(char *out,SBYTE dbname,SBYTE sName)
{
    const char *sql 	= "create table if not exists";
    const char *cName = SQL_GetTable_cName_(dbname,sName);
    const SQL_COL_S *pCol = SQL_GetTable_Col_(dbname,sName);
    SWORD sCol = SQL_GetTable_sColNum_(dbname,sName);
    char tem[SQL_CHAR_MAX_LARGE] = {0};
    SWORD i = 0;

    if (!cName) return;
    if (!pCol) return;

    strcat(tem,sql);
    strcat(tem," ");
    strcat(tem,cName);
    strcat(tem,"(");
		
    for (i = 0;i < sCol;i++)
    {
        strcat(tem,pCol[i].cName);
        strcat(tem," ");
        strcat(tem,pCol[i].type);	
        if (i < (sCol-1))
            strcat(tem,",");
    }
    strcat(tem,")");
    strcpy(out,tem);
    return;
}

int SQLTOOL_GetCountIntFromTable(SBYTE dbname,SBYTE sName,SBYTE sCol)
{
    const char *cName = SQL_GetTable_cName_(dbname,sName);
    const char *cCol = SQLTOOL_GetColcName(dbname,sName,sCol);
    char sql[512] = {0};
    sqlite3 *dbhandle = NULL;
    SBYTE ret = SQLITE_ERROR;
    int num = 0;

    if (!cName) return ret;
    if (!cCol) return ret;

    sprintf(sql,"select COUNT(%s) from %s where %s>0",cCol,cName,cCol);

    dbhandle = SQL_Connect_(dbname);
    ret = SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetRowID_,(int *)&num);
    SQL_Close_(dbname,dbhandle);

    return num;
}

//faster
BOOL SQLTOOL_TBIsEmpty_v2(SBYTE dbname,SBYTE sName)
{
    const char *cName = SQL_GetTable_cName_(dbname,sName);
    sqlite3 *dbhandle = NULL;
    char sql[512]={0};
    int iRow = 0;

    if (!cName) return TRUE;

    sprintf(sql,"select count(*) from %s",cName);

    dbhandle = SQL_Connect_(dbname);
    SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetColCount_,(SWORD *)&iRow);
    SQL_Close_(dbname,dbhandle);

    if (!iRow)
        return TRUE;
    return FALSE;
}

BOOL SQLTOOL_CheckTableIsVilid(SBYTE dbname,SBYTE sName)
{
    const char *cName = SQL_GetTable_cName_(dbname,sName);
    sqlite3 *dbhandle = NULL;
    char sql[512]={0};
    int iRow = 0;
    SBYTE ret = 0;

    if (!cName) return TRUE;

    sprintf(sql,"select count(*) from %s",cName);

    dbhandle = SQL_Connect_(dbname);
    ret = SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetColCount_,(SWORD *)&iRow);
    SQL_Close_(dbname,dbhandle);

    return ((ret==SQLITE_OK)?TRUE:FALSE);
}

BOOL SQLTOOL_TBIsEmpty_v2_c(SBYTE dbname,const char *cName)
{
    sqlite3 *dbhandle = NULL;
    char sql[512]={0};
    int iRow = 0;

    if (!cName) return TRUE;

    sprintf(sql,"select count(*) from '%s'",cName);

    dbhandle = SQL_Connect_(dbname);
    SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetColCount_,(SWORD *)&iRow);
    SQL_Close_(dbname,dbhandle);

    if (!iRow)
        return TRUE;
    return FALSE;
}


BOOL SQLTOOL_TBIsExist(SBYTE dbname,SBYTE sName)
{
    const char *cName = SQL_GetTable_cName_(dbname,sName);
    char sql[512]={0};
    sqlite3 *dbhandle = NULL;
    SWORD count = 0;

    if (!cName) return FALSE;

    sprintf(sql,"select count(*) from sqlite_master where type='table' and name='%s'",cName);
    dbhandle = SQL_Connect_(dbname);
    SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetTableCount_,(SWORD *)&count);
    SQL_Close_(dbname,dbhandle);

    if(count)	 
        return TRUE;
    return FALSE;
}

BOOL SQLTOOL_TBIsExist_c(SBYTE dbname,const char *cName)
{
    char sql[512]={0};
    sqlite3 *dbhandle = NULL;
    SWORD count = 0;

    if (!cName) return FALSE;

    sprintf(sql,"select count(*) from sqlite_master where type='table' and name='%s'",cName);
    dbhandle = SQL_Connect_(dbname);
    SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetTableCount_,(SWORD *)&count);
    SQL_Close_(dbname,dbhandle);

    if(count)	 
        return TRUE;
    return FALSE;
}

BOOL SQLTOOL_TBIsExist_v2(SBYTE dbname,char (*TBSum)[64],const char *cName,SBYTE sName)
{
    SWORD 	count 		= 0;
    const char 	*NameTmp 	= NULL;
    BOOL 	flag 		= FALSE;

    if (!cName)
        NameTmp = SQL_GetTable_cName_(dbname,sName);
    else
        NameTmp = cName;

    while(OsPackStrLength_r(TBSum[count]))
    {
        if (0 == strcmp(NameTmp,TBSum[count])){
            flag = TRUE;
            break;
        }

        count ++;
        if (count == SQLTB_MAX_TBNUM)
        break;
    }
    return flag;
}


SWORD SQLTOOL_GetTBNames(SBYTE dbname,char (*para)[64])
{
    const char *sql = "select name from sqlite_master where type='table'";
    sqlite3 *dbhandle = NULL;
    SWORD num = 0;

    dbhandle = SQL_Connect_(dbname);
    SQL_Exec_(dbname,SQLOP_SELECT,dbhandle,sql,SQL_CallbackGetName_,para);
    SQL_Close_(dbname,dbhandle);
    num = _Name_Num_;
    _Name_Num_ = 0;
    return num;
}
#endif

#ifndef _SQL_OPERATE_
#define _SQL_OPERATE_

#ifndef _SQL_TRANSACTION_
#define _SQL_TRANSACTION_

SBYTE SQL_TransactionBegin(SBYTE dbname,sqlite3 **Tdbhandle)
{
    const char *sqlbgn = "begin";
    sqlite3 *dbhandle = NULL;

    dbhandle = SQL_Connect_(dbname);
    *Tdbhandle = dbhandle;
    return SQL_Exec_(dbname,SQLOP_TRABGN,dbhandle,sqlbgn,NULL,NULL);
}

SBYTE SQL_TransactionCommit(SBYTE dbname,sqlite3 *dbhandle)
{
    const char *sqlcom = "commit";
    SBYTE ret = SQLITE_ERROR;

    if (!dbhandle) return ret;

    ret = SQL_Exec_(dbname,SQLOP_TRACOM,dbhandle,sqlcom,NULL,NULL);
    SQL_Close_(dbname,dbhandle);
    return ret;
}

SBYTE SQL_TransactionRollback(SBYTE dbname,sqlite3 *dbhandle)
{
    const char *sqlrbk = "rollback";
    SBYTE ret = SQLITE_ERROR;

    if (!dbhandle) return ret;

    ret = SQL_Exec_(dbname,SQLOP_TRARBK,dbhandle,sqlrbk,NULL,NULL);
    SQL_Close_(dbname,dbhandle);
    return ret;
}

SBYTE SQL_TransactionExec(SBYTE dbname,SBYTE SQLOP,sqlite3 *dbhandle,const char *sql, int (*callback)(void*,int,char**,char**),void *arg)
{
    return SQL_Exec_(dbname,SQLOP,dbhandle,sql,callback,arg);
}
#endif


SBYTE SQL_DeleteData(SBYTE dbname,SBYTE sName,SBYTE icol2idx,int data2Idx,int RowID)
{
    const char *cName = SQL_GetTable_cName_(dbname,sName);
    const char *cCol = SQLTOOL_GetColcName(dbname,sName,icol2idx);
    char sql[128] = {0};
    sqlite3 *dbhandle = NULL;
    SBYTE ret = SQLITE_ERROR;	

    if (!cName) return ret;

    if (RowID)
        sprintf(sql,"delete from %s where ROWID=%d",cName,RowID);
    else
        sprintf(sql,"delete from %s where %s=%d",cName,cCol,data2Idx);

    dbhandle = SQL_Connect_(dbname);
    ret = SQL_Exec_(dbname,SQLOP_DELETE,dbhandle,sql,NULL,NULL);	
    SQL_Close_(dbname,dbhandle);
    return ret;
}


SBYTE SQL_DropTab(SBYTE dbname,SBYTE sName,const char *cName)
{
    char sql[128] = {0};
    sqlite3 *dbhandle = NULL;
    SBYTE ret = SQLITE_ERROR;

    if (cName != NULL)
    {
        sprintf(sql,"drop table if exists %s",cName);
    }
    else
    {
        const char *tableName = SQL_GetTable_cName_(dbname,sName);
        if (!tableName) return ret;
        sprintf(sql,"drop table if exists %s",tableName);
    }
	
    dbhandle = SQL_Connect_(dbname);
    ret = SQL_Exec_(dbname,SQLOP_DROPTB,dbhandle,sql,NULL,NULL);
    SQL_Close_(dbname,dbhandle);
    return ret;
}

SBYTE SQL_CopyTab(SBYTE dbname,const char *NewTB,const char *OldTB)
{
    char sql[128] = {0};
    sqlite3 *dbhandle = NULL;
    SBYTE ret = SQLITE_ERROR;

    if (!NewTB) return ret;
    if (!OldTB) return ret;

    sprintf(sql,"create table '%s' as select * from '%s'",NewTB,OldTB);
    dbhandle = SQL_Connect_(dbname);
    ret = SQL_Exec_(dbname,SQLOP_COPYTB,dbhandle,sql,NULL,NULL);
    SQL_Close_(dbname,dbhandle);
    return ret;
}

BOOL SQL_DelDataBase(const char *url)
{
    char cmdline[64] = {0};
    sprintf(cmdline,"rm -f %s",url);
    system(cmdline);
    return TRUE;
}

BOOL SQL_BackUpDatabase(const char *url)
{
    char cmdline[64] = {0};
    sprintf(cmdline,"cp -rf %s %s",url,PATHNAME_SQLBACK_VAR);
    system(cmdline);
    return TRUE;
}

BOOL SQL_BackUpDataBase_Var()
{
    char cmdline[64] = {0};
    sprintf(cmdline,"cp -rf %s %s",PATHNAME_SQLVAR,PATHNAME_SQLBACK);
    system(cmdline);
    return TRUE;
}

BOOL SQL_BackUpDataBase_Data()
{
    char cmdline[64] = {0};
    sprintf(cmdline,"cp -rf %s %s",PATHNAME_SQLDATA,PATHNAME_SQLBACK);
    system(cmdline);
    return TRUE;
}

BOOL SQL_RecoveryTable(SBYTE dbname,SBYTE sName)
{
    char *tablename = SQL_GetTable_cName_(dbname,sName);
    char *url = SQL_DB[dbname].url;
    char attach[128] = {0};
    char detach[128] = {0};
    char sql[128] = {0};
    char bksrc[128] = {0};
    sqlite3 *dbhandle 	= NULL;
    SBYTE ret1=0,ret2=0,ret3=0;

    OsPackStrCopy_r(bksrc,PATHNAME_SQLBACK_VAR);
    OsPackStrAppend_r(bksrc,"/");
    OsPackStrAppend_r(bksrc,SQLTOOL_DebugGetDataBaseName(url));

    sprintf(attach,"ATTACH DATABASE \'%s\' as T",bksrc);
    sprintf(sql,"create table %s as select * from T.%s",tablename,tablename);
    sprintf(detach,"DETACH DATABASE T");

    if (!IsFileExist(bksrc))
    {
        return FALSE;
    }
    else
    {
        SBYTE eret1=0, eret2= 0,eret3=0;
        char esql[64] = {0};
        sqlite3 * ehandle = NULL;
        int eRow = 0;

        sprintf(esql,"select count(*) from %s",tablename);

        eret1 = sqlite3_open(bksrc,&ehandle);
        eret2 = SQL_Exec_(dbname,SQLOP_SELECT,ehandle,esql,SQL_CallbackGetColCount_,(SWORD *)&eRow);
        eret3 = sqlite3_close(ehandle);

        if ((eret1 != SQLITE_OK) || (eret2 != SQLITE_OK)||(eret3 != SQLITE_OK)||(eRow <= 0))
        {
            FileDelete(bksrc);
            return FALSE;
        }
    }
	
    dbhandle = SQL_Connect_(dbname);
    ret1 = SQL_Exec_(dbname,SQLOP_ATTACH,dbhandle,attach,NULL,NULL);
    ret2 = SQL_Exec_(dbname,SQLOP_CREATE,dbhandle,sql,NULL,NULL);
    ret3 = SQL_Exec_(dbname,SQLOP_ATTACH,dbhandle,detach,NULL,NULL);
    SQL_Close_(dbname,dbhandle);

    if ((ret1 != SQLITE_OK) || (ret2 != SQLITE_OK)||(ret3 != SQLITE_OK))
    {
        return FALSE;
    }
    return TRUE;
}

static void SQL_BackUpInfo2Log(void)
{
    char cmdline[128] = {0};
    sprintf(cmdline,"echo \"BackUpInfo at `date`\" >>%s",PATHNAME_LOG"/BackUp.log");
    system(cmdline);
    return;
}

BOOL SQL_BackUpAutoProcess(void)
{
    const char *src = PATHNAME_SQLVAR;
    const char *bk = PATHNAME_SQLBACK;
    static ATIME Lasttime;
    ATIME now = 0;
    char cmdline[128] = {0};

    OsPackGetAnsiTime_r(&now);

    if (Lasttime == 0)
    {
        ;
    }
    else
    {
        if ((now - Lasttime) < SQLBACKUP_TIME_INTERVAL)
        {
            return TRUE;
        }
    }
	
    OsPackGetAnsiTime_r(&Lasttime);
    sprintf(cmdline,"cp -rf %s %s",src,bk);
    system(cmdline);

    SQL_BackUpInfo2Log();

    return TRUE;
}

SBYTE SQL_DeleteTab(SBYTE dbname,const char *cName,SBYTE sName)
{
    char sql[128] 	= {0};
    sqlite3 *dbhandle 			= NULL;
    const char *NameTmp 				= NULL;
    SBYTE ret 					= SQLITE_ERROR;

    if (!cName)
    {
        NameTmp = SQL_GetTable_cName_(dbname,sName);
    }
    else
    {
        NameTmp = cName;
    }

    sprintf(sql,"delete from '%s'",NameTmp);

    dbhandle = SQL_Connect_(dbname);
    ret = SQL_Exec_(dbname,SQLOP_DELETE,dbhandle,sql,NULL,NULL);
    SQL_Close_(dbname,dbhandle);	
    return ret;
}
#endif
#endif
#endif

