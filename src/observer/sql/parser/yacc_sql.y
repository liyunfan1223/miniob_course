
%{

#include "sql/parser/parse_defs.h"
#include "sql/parser/yacc_sql.tab.h"
#include "sql/parser/lex.yy.h"
// #include "common/log/log.h" // 包含C++中的头文件

#include<stdio.h>
#include<stdlib.h>
#include<string.h>

typedef struct ParserContext {
  Query * ssql;
  size_t select_length;
  size_t condition_length;
  size_t from_length;
  size_t value_length;
  Value values[MAX_NUM];
  size_t record_length;
  InsertRecord records[MAX_NUM];
  Condition conditions[MAX_NUM];
  CompOp comp;
  char id[MAX_NUM];
  OrderType order_type;
  AggType agg_type;
  size_t attr_length;
} ParserContext;

//获取子串
char *substr(const char *s,int n1,int n2)/*从s中提取下标为n1~n2的字符组成一个新字符串，然后返回这个新串的首地址*/
{
  char *sp = malloc(sizeof(char) * (n2 - n1 + 2));
  int i, j = 0;
  for (i = n1; i <= n2; i++) {
    sp[j++] = s[i];
  }
  sp[j] = 0;
  return sp;
}

void yyerror(yyscan_t scanner, const char *str)
{
  ParserContext *context = (ParserContext *)(yyget_extra(scanner));
  query_reset(context->ssql);
  context->ssql->flag = SCF_ERROR;
  context->condition_length = 0;
  context->from_length = 0;
  context->select_length = 0;
  context->value_length = 0;
  context->ssql->sstr.insertion.record_num = 0;
  printf("parse sql failed. error=%s", str);
}

ParserContext *get_context(yyscan_t scanner)
{
  return (ParserContext *)yyget_extra(scanner);
}

#define CONTEXT get_context(scanner)


%}

%define api.pure full
%lex-param { yyscan_t scanner }
%parse-param { void *scanner }

//标识tokens
%token  SEMICOLON
        CREATE
        DROP
        TABLE
        TABLES
        INDEX
        SELECT
        DESC
        SHOW
        SYNC
        INSERT
        DELETE
        UPDATE
        LBRACE
        RBRACE
        COMMA
        TRX_BEGIN
        TRX_COMMIT
        TRX_ROLLBACK
        INT_T
        STRING_T
        FLOAT_T
        DATE_T
        HELP
        EXIT
        DOT //QUOTE
        INTO
        VALUES
        FROM
        WHERE
        AND
        SET
        ON
        LOAD
        DATA
        INFILE
        EQ
        LT
        GT
        LE
        GE
        NE
        INNER
        JOIN
        MAX
        MIN
        COUNT
        AVG
        SUM
        GROUP
        BY
        ORDER
        ASC
        NOT
        LIKE
        TEXT_T
        NULL_TOKEN
        NULLABLE
        HAVING

%union {
  struct _Attr *attr;
  struct _Condition *condition1;
  struct _Value *value1;
  char *string;
  int number;
  float floats;
	char *position;
}

%token <number> NUMBER
%token <floats> FLOAT
%token <string> DATE_STR
%token <string> ID
%token <string> PATH
%token <string> SSS
%token <string> STAR
%token <string> STRING_V
//非终结符

%type <number> type;
%type <condition1> condition;
%type <value1> value;
%type <number> number;

%%

commands:		//commands or sqls. parser starts here.
    /* empty */
    | commands command
    ;

command:
	  select  
	| insert
	| update
	| delete
	| create_table
	| drop_table
	| show_tables
	| desc_table
	| create_index	
	| drop_index
	| sync
	| begin
	| commit
	| rollback
	| load_data
	| help
	| exit
	| show_index
    ;

show_index:
    SHOW INDEX FROM ID SEMICOLON {
	CONTEXT->ssql->flag=SCF_SHOW_INDEX;
	show_index_init(&CONTEXT->ssql->sstr.show_index, $4);
    };

exit:			
    EXIT SEMICOLON {
        CONTEXT->ssql->flag=SCF_EXIT;//"exit";
    };

help:
    HELP SEMICOLON {
        CONTEXT->ssql->flag=SCF_HELP;//"help";
    };

sync:
    SYNC SEMICOLON {
      CONTEXT->ssql->flag = SCF_SYNC;
    }
    ;

begin:
    TRX_BEGIN SEMICOLON {
      CONTEXT->ssql->flag = SCF_BEGIN;
    }
    ;

commit:
    TRX_COMMIT SEMICOLON {
      CONTEXT->ssql->flag = SCF_COMMIT;
    }
    ;

rollback:
    TRX_ROLLBACK SEMICOLON {
      CONTEXT->ssql->flag = SCF_ROLLBACK;
    }
    ;

drop_table:		/*drop table 语句的语法解析树*/
    DROP TABLE ID SEMICOLON {
        CONTEXT->ssql->flag = SCF_DROP_TABLE;//"drop_table";
        drop_table_init(&CONTEXT->ssql->sstr.drop_table, $3);
    };

show_tables:
    SHOW TABLES SEMICOLON {
      CONTEXT->ssql->flag = SCF_SHOW_TABLES;
    }
    ;

desc_table:
    DESC ID SEMICOLON {
      CONTEXT->ssql->flag = SCF_DESC_TABLE;
      desc_table_init(&CONTEXT->ssql->sstr.desc_table, $2);
    }
    ;

create_index:		/*create index 语句的语法解析树*/
    CREATE INDEX ID ON ID LBRACE ID RBRACE SEMICOLON 
		{
			CONTEXT->ssql->flag = SCF_CREATE_INDEX;//"create_index";
			create_index_init(&CONTEXT->ssql->sstr.create_index, $3, $5, $7);
		}
    ;

drop_index:			/*drop index 语句的语法解析树*/
    DROP INDEX ID  SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_DROP_INDEX;//"drop_index";
			drop_index_init(&CONTEXT->ssql->sstr.drop_index, $3);
		}
    ;
create_table:		/*create table 语句的语法解析树*/
    CREATE TABLE ID LBRACE attr_def attr_def_list RBRACE SEMICOLON 
		{
			CONTEXT->ssql->flag=SCF_CREATE_TABLE;//"create_table";
			// CONTEXT->ssql->sstr.create_table.attribute_count = CONTEXT->value_length;
			create_table_init_name(&CONTEXT->ssql->sstr.create_table, $3);
			//临时变量清零	
			CONTEXT->value_length = 0;
		}
    ;
attr_def_list:
    /* empty */
    | COMMA attr_def attr_def_list {    }
    ;
    
attr_def:
    ID_get type LBRACE number RBRACE 
	{
		AttrInfo attribute;
		attr_info_init(&attribute, CONTEXT->id, $2, $4);
		create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name =(char*)malloc(sizeof(char));
		// strcpy(CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name, CONTEXT->id);
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].type = $2;
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].length = $4;
		CONTEXT->value_length++;
	}
    |ID_get type
	{
		AttrInfo attribute;
		attr_info_init(&attribute, CONTEXT->id, $2, CONTEXT->attr_length);
		create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name=(char*)malloc(sizeof(char));
		// strcpy(CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name, CONTEXT->id);
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].type=$2;
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].length=4; // default attribute length
		CONTEXT->value_length++;
	}
	|ID_get type NOT NULL_TOKEN
    	{
    		AttrInfo attribute;
    		attr_info_init(&attribute, CONTEXT->id, $2, CONTEXT->attr_length);
    		create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
    		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name=(char*)malloc(sizeof(char));
    		// strcpy(CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name, CONTEXT->id);
    		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].type=$2;
    		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].length=4; // default attribute length
    		CONTEXT->value_length++;
    	}
    |ID_get type NULLABLE
	{
		AttrInfo attribute;
		nullable_attr_info_init(&attribute, CONTEXT->id, $2, CONTEXT->attr_length);
		create_table_append_attribute(&CONTEXT->ssql->sstr.create_table, &attribute);
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name=(char*)malloc(sizeof(char));
		// strcpy(CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].name, CONTEXT->id);
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].type=$2;
		// CONTEXT->ssql->sstr.create_table.attributes[CONTEXT->value_length].length=4; // default attribute length
		CONTEXT->value_length++;
	}
    ;
number:
		NUMBER {$$ = $1;}
		;
type:
	INT_T { $$=INTS; CONTEXT->attr_length = 4; }
       | STRING_T { $$=CHARS; CONTEXT->attr_length = 40; }
       | FLOAT_T { $$=FLOATS; CONTEXT->attr_length = 4; }
       | DATE_T { $$=DATES; CONTEXT->attr_length = 4; }
       | TEXT_T { $$=TEXTS; CONTEXT->attr_length = 4; }
       ;

ID_get:
	ID 
	{
		char *temp=$1; 
		snprintf(CONTEXT->id, sizeof(CONTEXT->id), "%s", temp);
	}
	;

	
insert:				/*insert   语句的语法解析树*/
    INSERT INTO ID VALUES record_list SEMICOLON
    {
      CONTEXT->ssql->flag=SCF_INSERT;//"insert";
      inserts_init(&CONTEXT->ssql->sstr.insertion, $3, CONTEXT->records, CONTEXT->record_length);
      //临时变量清零
      CONTEXT->record_length = 0;
    }
record_list:
    /* empty */
    | COMMA record record_list {
    }
    | record record_list {
    }
    ;
record:
    LBRACE value_list RBRACE {
    	insert_record_init(&CONTEXT->records[CONTEXT->record_length++], CONTEXT->values, CONTEXT->value_length);
    	CONTEXT->value_length=0;
    }
    ;
value_list:
    /* empty */
    | COMMA value value_list  {
    }
    | value value_list {
    }
value:
    NUMBER {
  		value_init_integer(&CONTEXT->values[CONTEXT->value_length++], $1);
    }
    |FLOAT {
  		value_init_float(&CONTEXT->values[CONTEXT->value_length++], $1);
    }
    |DATE_STR {
    		$1 = substr($1,1,strlen($1)-2);
		value_init_date(&CONTEXT->values[CONTEXT->value_length++], $1);
    }
    |SSS {
		$1 = substr($1,1,strlen($1)-2);
  		value_init_string(&CONTEXT->values[CONTEXT->value_length++], $1);
    }
    |NULL_TOKEN {
    		value_init_null(&CONTEXT->values[CONTEXT->value_length++]);
    }
    ;
    
delete:		/*  delete 语句的语法解析树*/
    DELETE FROM ID where SEMICOLON 
		{
			CONTEXT->ssql->flag = SCF_DELETE;//"delete";
			deletes_init_relation(&CONTEXT->ssql->sstr.deletion, $3);
			deletes_set_conditions(&CONTEXT->ssql->sstr.deletion, 
					CONTEXT->conditions, CONTEXT->condition_length);
			CONTEXT->condition_length = 0;	
    }
    ;
update:			/*  update 语句的语法解析树*/
    UPDATE ID SET ID EQ value where SEMICOLON
		{
			CONTEXT->ssql->flag = SCF_UPDATE;//"update";
			Value *value = &CONTEXT->values[0];
			updates_init(&CONTEXT->ssql->sstr.update, $2, $4, value, 
					CONTEXT->conditions, CONTEXT->condition_length);
			CONTEXT->condition_length = 0;
		}
    ;
select:				/*  select 语句的语法解析树*/
    SELECT attr_list FROM ID inner_join_list rel_list where group order SEMICOLON
		{
			// CONTEXT->ssql->sstr.selection.relations[CONTEXT->from_length++]=$4;
			selects_append_relation(&CONTEXT->ssql->sstr.selection, $4);

			selects_append_conditions(&CONTEXT->ssql->sstr.selection, CONTEXT->conditions, CONTEXT->condition_length);

			CONTEXT->ssql->flag=SCF_SELECT;//"select";
			// CONTEXT->ssql->sstr.selection.attr_num = CONTEXT->select_length;

			//临时变量清零
			CONTEXT->condition_length=0;
			CONTEXT->from_length=0;
			CONTEXT->select_length=0;
			CONTEXT->value_length = 0;
	}
	;

attr_list:
    /* empty */
    | COMMA select_attr attr_list {
    }
    | select_attr attr_list {
    }
    ;

select_attr:
    STAR {  
	RelAttr attr;
	relation_attr_init(&attr, NULL, "*");
 	selects_append_attribute(&CONTEXT->ssql->sstr.selection, &attr);
    }
    | ID {
        RelAttr attr;
	relation_attr_init(&attr, NULL, $1);
	selects_append_attribute(&CONTEXT->ssql->sstr.selection, &attr);
    }
    | ID DOT ID {
        RelAttr attr;
	relation_attr_init(&attr, $1, $3);
	selects_append_attribute(&CONTEXT->ssql->sstr.selection, &attr);
    }
    | aggop LBRACE STAR RBRACE {
	RelAttr attr;
	relation_attr_aggr_init(&attr, NULL, "*", CONTEXT->agg_type);
	selects_append_attribute(&CONTEXT->ssql->sstr.selection, &attr);
    }
    | aggop LBRACE ID RBRACE {
    	RelAttr attr;
    	relation_attr_aggr_init(&attr, NULL, $3, CONTEXT->agg_type);
    	selects_append_attribute(&CONTEXT->ssql->sstr.selection, &attr);
    }
    | aggop LBRACE ID DOT ID RBRACE {
    	RelAttr attr;
    	relation_attr_aggr_init(&attr, $3, $5, CONTEXT->agg_type);
    	selects_append_attribute(&CONTEXT->ssql->sstr.selection, &attr);
    }
    ;
aggop:
    MAX {
	CONTEXT->agg_type = AGG_MAX;
    }
    | MIN {
	CONTEXT->agg_type = AGG_MIN;
    }
    | COUNT {
	CONTEXT->agg_type = AGG_COUNT;
    }
    | AVG {
	CONTEXT->agg_type = AGG_AVG;
    }
    | SUM {
    	CONTEXT->agg_type = AGG_SUM;
    }
    ;
inner_join_list:
    /* empty */
    | INNER JOIN ID on inner_join_list {
	selects_append_relation(&CONTEXT->ssql->sstr.selection, $3);
    }

on:
    /* empty */
    | ON condition condition_list {
	// CONTEXT->conditions[CONTEXT->condition_length++]=*$2;
    }
    ;



rel_list:
    /* empty */
    | COMMA ID rel_list {	
				selects_append_relation(&CONTEXT->ssql->sstr.selection, $2);
		  }
    ;
where:
    /* empty */ 
    | WHERE condition condition_list {	
				// CONTEXT->conditions[CONTEXT->condition_length++]=*$2;
			}
    ;
condition_list:
    /* empty */
    | AND condition condition_list {
				// CONTEXT->conditions[CONTEXT->condition_length++]=*$2;
			}
    ;
condition:
    ID comOp value 
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, NULL, $1);
			Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];
			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
		}
		|value comOp value 
		{
			Value *left_value = &CONTEXT->values[CONTEXT->value_length - 2];
			Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 0, NULL, left_value, 0, NULL, right_value);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
		}
		|ID comOp ID 
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, NULL, $1);
			RelAttr right_attr;
			relation_attr_init(&right_attr, NULL, $3);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
		}
    |value comOp ID
		{
			Value *left_value = &CONTEXT->values[CONTEXT->value_length - 1];
			RelAttr right_attr;
			relation_attr_init(&right_attr, NULL, $3);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 0, NULL, left_value, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
		}
    |ID DOT ID comOp value
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, $1, $3);
			Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;

			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 1;
			// $$->left_attr.relation_name=$1;
			// $$->left_attr.attribute_name=$3;
			// $$->comp=CONTEXT->comp;
			// $$->right_is_attr = 0;   //属性值
			// $$->right_attr.relation_name=NULL;
			// $$->right_attr.attribute_name=NULL;
			// $$->right_value =*$5;			
							
    }
    |value comOp ID DOT ID
		{
			Value *left_value = &CONTEXT->values[CONTEXT->value_length - 1];

			RelAttr right_attr;
			relation_attr_init(&right_attr, $3, $5);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 0, NULL, left_value, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
			// $$=( Condition *)malloc(sizeof( Condition));
			// $$->left_is_attr = 0;//属性值
			// $$->left_attr.relation_name=NULL;
			// $$->left_attr.attribute_name=NULL;
			// $$->left_value = *$1;
			// $$->comp =CONTEXT->comp;
			// $$->right_is_attr = 1;//属性
			// $$->right_attr.relation_name = $3;
			// $$->right_attr.attribute_name = $5;
									
    }
    |ID DOT ID comOp ID DOT ID
		{
			RelAttr left_attr;
			relation_attr_init(&left_attr, $1, $3);
			RelAttr right_attr;
			relation_attr_init(&right_attr, $5, $7);

			Condition condition;
			condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 1, &right_attr, NULL);
			CONTEXT->conditions[CONTEXT->condition_length++] = condition;
    }
    ;
group:
    /* empty */
    | GROUP BY group_attr_list {
    }
    | GROUP BY group_attr_list HAVING hc_list {

    }
    ;
group_attr_list:
    /* empty */
    | COMMA group_attr group_attr_list {
    }
    | group_attr group_attr_list {
    }
    ;
group_attr:
    ID {
        GroupAttr group_attr;
	group_attr_init(&group_attr, NULL, $1);
	group_append_attribute(&CONTEXT->ssql->sstr.selection, &group_attr);
    }
    | ID DOT ID {
        GroupAttr group_attr;
        group_attr_init(&group_attr, $1, $3);
        group_append_attribute(&CONTEXT->ssql->sstr.selection, &group_attr);
    }
    ;

hc_list:
    /* empty */
    | having_condition hc_list {

    }
    | AND having_condition hc_list {
    }
    ;

having_condition:
    aggop LBRACE ID RBRACE comOp value {
        RelAttr left_attr;
        relation_attr_aggr_init(&left_attr, NULL, $3, CONTEXT->agg_type);
        Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];
        Condition condition;
        condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
        selects_append_having_condition(&CONTEXT->ssql->sstr.selection, &condition);
        CONTEXT->value_length = 0;
    }
    | aggop LBRACE ID DOT ID RBRACE comOp value {
        RelAttr left_attr;
        relation_attr_aggr_init(&left_attr, $3, $5, CONTEXT->agg_type);
        Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];
        Condition condition;
        condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
        selects_append_having_condition(&CONTEXT->ssql->sstr.selection, &condition);
        CONTEXT->value_length = 0;
    }
    | aggop LBRACE STAR RBRACE comOp value {
        RelAttr left_attr;
        relation_attr_aggr_init(&left_attr, NULL, "*", CONTEXT->agg_type);
        Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];
        Condition condition;
        condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
        selects_append_having_condition(&CONTEXT->ssql->sstr.selection, &condition);
        CONTEXT->value_length = 0;
    }
    | ID comOp value {
        RelAttr left_attr;
        relation_attr_init(&left_attr, NULL, $1);
        Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];
        Condition condition;
        condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
        selects_append_having_condition(&CONTEXT->ssql->sstr.selection, &condition);
        CONTEXT->value_length = 0;
    }
    | ID DOT ID comOp value {
        RelAttr left_attr;
        relation_attr_aggr_init(&left_attr, $1, $3);
        Value *right_value = &CONTEXT->values[CONTEXT->value_length - 1];
        Condition condition;
        condition_init(&condition, CONTEXT->comp, 1, &left_attr, NULL, 0, NULL, right_value);
        selects_append_having_condition(&CONTEXT->ssql->sstr.selection, &condition);
        CONTEXT->value_length = 0;
    }


order:
    /* empty */
    | ORDER BY order_attr_list {
    }
    ;
order_attr_list:
    /* empty */
    | COMMA order_attr order_attr_list {
    }
    | order_attr order_attr_list {
    }
    ;
order_attr:
    ID order_type {
        OrderAttr order_attr;
	order_attr_init(&order_attr, NULL, $1, CONTEXT->order_type);
	order_append_attribute(&CONTEXT->ssql->sstr.selection, &order_attr);
    }
    | ID DOT ID order_type {
        OrderAttr order_attr;
	order_attr_init(&order_attr, $1, $3, CONTEXT->order_type);
	order_append_attribute(&CONTEXT->ssql->sstr.selection, &order_attr);
    }
    ;
order_type:
    ASC {
    	CONTEXT->order_type = OrderAsc;
    }
    | DESC {
    	CONTEXT->order_type = OrderDesc;
    }
    | /* empty */ {
    	CONTEXT->order_type = OrderAsc;
    }
comOp:
  	  EQ { CONTEXT->comp = EQUAL_TO; }
    | LT { CONTEXT->comp = LESS_THAN; }
    | GT { CONTEXT->comp = GREAT_THAN; }
    | LE { CONTEXT->comp = LESS_EQUAL; }
    | GE { CONTEXT->comp = GREAT_EQUAL; }
    | NE { CONTEXT->comp = NOT_EQUAL; }
    | LIKE { CONTEXT->comp = LIKE_TO; }
    | NOT LIKE { CONTEXT->comp = NOT_LIKE_TO; }
    ;

load_data:
		LOAD DATA INFILE SSS INTO TABLE ID SEMICOLON
		{
		  CONTEXT->ssql->flag = SCF_LOAD_DATA;
			load_data_init(&CONTEXT->ssql->sstr.load_data, $7, $4);
		}
		;
%%
//_____________________________________________________________________
extern void scan_string(const char *str, yyscan_t scanner);

int sql_parse(const char *s, Query *sqls){
	ParserContext context;
	memset(&context, 0, sizeof(context));

	yyscan_t scanner;
	yylex_init_extra(&context, &scanner);
	context.ssql = sqls;
	scan_string(s, scanner);
	int result = yyparse(scanner);
	yylex_destroy(scanner);
	return result;
}