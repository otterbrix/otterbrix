/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_BASE_YY_GRAM_HPP_INCLUDED
#define YY_BASE_YY_GRAM_HPP_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
#define YYDEBUG 0
#endif
#if YYDEBUG
extern int base_yydebug;
#endif

/* Token kinds.  */
#ifndef YYTOKENTYPE
#define YYTOKENTYPE
enum yytokentype
{
    YYEMPTY = -2,
    YYEOF = 0,                 /* "end of file"  */
    YYerror = 256,             /* error  */
    YYUNDEF = 257,             /* "invalid token"  */
    IDENT = 258,               /* IDENT  */
    FCONST = 259,              /* FCONST  */
    SCONST = 260,              /* SCONST  */
    BCONST = 261,              /* BCONST  */
    XCONST = 262,              /* XCONST  */
    Op = 263,                  /* Op  */
    ICONST = 264,              /* ICONST  */
    PARAM = 265,               /* PARAM  */
    TYPECAST = 266,            /* TYPECAST  */
    DOT_DOT = 267,             /* DOT_DOT  */
    COLON_EQUALS = 268,        /* COLON_EQUALS  */
    ABORT_P = 269,             /* ABORT_P  */
    ABSOLUTE_P = 270,          /* ABSOLUTE_P  */
    ACCESS = 271,              /* ACCESS  */
    ACTION = 272,              /* ACTION  */
    ADD_P = 273,               /* ADD_P  */
    ADMIN = 274,               /* ADMIN  */
    AFTER = 275,               /* AFTER  */
    AGGREGATE = 276,           /* AGGREGATE  */
    ALL = 277,                 /* ALL  */
    ALSO = 278,                /* ALSO  */
    ALTER = 279,               /* ALTER  */
    ALWAYS = 280,              /* ALWAYS  */
    ANALYSE = 281,             /* ANALYSE  */
    ANALYZE = 282,             /* ANALYZE  */
    AND = 283,                 /* AND  */
    ANY = 284,                 /* ANY  */
    ARRAY = 285,               /* ARRAY  */
    AS = 286,                  /* AS  */
    ASC = 287,                 /* ASC  */
    ASSERTION = 288,           /* ASSERTION  */
    ASSIGNMENT = 289,          /* ASSIGNMENT  */
    ASYMMETRIC = 290,          /* ASYMMETRIC  */
    AT = 291,                  /* AT  */
    ATTRIBUTE = 292,           /* ATTRIBUTE  */
    AUTHORIZATION = 293,       /* AUTHORIZATION  */
    BACKWARD = 294,            /* BACKWARD  */
    BEFORE = 295,              /* BEFORE  */
    BEGIN_P = 296,             /* BEGIN_P  */
    BETWEEN = 297,             /* BETWEEN  */
    BIGINT = 298,              /* BIGINT  */
    BINARY = 299,              /* BINARY  */
    BIT = 300,                 /* BIT  */
    BOOLEAN_P = 301,           /* BOOLEAN_P  */
    BOTH = 302,                /* BOTH  */
    BY = 303,                  /* BY  */
    CACHE = 304,               /* CACHE  */
    CALLED = 305,              /* CALLED  */
    CASCADE = 306,             /* CASCADE  */
    CASCADED = 307,            /* CASCADED  */
    CASE = 308,                /* CASE  */
    CAST = 309,                /* CAST  */
    CATALOG_P = 310,           /* CATALOG_P  */
    CHAIN = 311,               /* CHAIN  */
    CHAR_P = 312,              /* CHAR_P  */
    CHARACTER = 313,           /* CHARACTER  */
    CHARACTERISTICS = 314,     /* CHARACTERISTICS  */
    CHECK = 315,               /* CHECK  */
    CHECKPOINT = 316,          /* CHECKPOINT  */
    CLASS = 317,               /* CLASS  */
    CLOSE = 318,               /* CLOSE  */
    CLUSTER = 319,             /* CLUSTER  */
    COALESCE = 320,            /* COALESCE  */
    COLLATE = 321,             /* COLLATE  */
    COLLATION = 322,           /* COLLATION  */
    COLUMN = 323,              /* COLUMN  */
    COMMENT = 324,             /* COMMENT  */
    COMMENTS = 325,            /* COMMENTS  */
    COMMIT = 326,              /* COMMIT  */
    COMMITTED = 327,           /* COMMITTED  */
    CONCURRENCY = 328,         /* CONCURRENCY  */
    CONCURRENTLY = 329,        /* CONCURRENTLY  */
    CONFIGURATION = 330,       /* CONFIGURATION  */
    CONNECTION = 331,          /* CONNECTION  */
    CONSTRAINT = 332,          /* CONSTRAINT  */
    CONSTRAINTS = 333,         /* CONSTRAINTS  */
    CONTENT_P = 334,           /* CONTENT_P  */
    CONTINUE_P = 335,          /* CONTINUE_P  */
    CONVERSION_P = 336,        /* CONVERSION_P  */
    COPY = 337,                /* COPY  */
    COST = 338,                /* COST  */
    CREATE = 339,              /* CREATE  */
    CROSS = 340,               /* CROSS  */
    CSV = 341,                 /* CSV  */
    CURRENT_P = 342,           /* CURRENT_P  */
    CURRENT_CATALOG = 343,     /* CURRENT_CATALOG  */
    CURRENT_DATE = 344,        /* CURRENT_DATE  */
    CURRENT_ROLE = 345,        /* CURRENT_ROLE  */
    CURRENT_SCHEMA = 346,      /* CURRENT_SCHEMA  */
    CURRENT_TIME = 347,        /* CURRENT_TIME  */
    CURRENT_TIMESTAMP = 348,   /* CURRENT_TIMESTAMP  */
    CURRENT_USER = 349,        /* CURRENT_USER  */
    CURSOR = 350,              /* CURSOR  */
    CYCLE = 351,               /* CYCLE  */
    DATA_P = 352,              /* DATA_P  */
    DATABASE = 353,            /* DATABASE  */
    DAY_P = 354,               /* DAY_P  */
    DEALLOCATE = 355,          /* DEALLOCATE  */
    DEC = 356,                 /* DEC  */
    DECIMAL_P = 357,           /* DECIMAL_P  */
    DECLARE = 358,             /* DECLARE  */
    DEFAULT = 359,             /* DEFAULT  */
    DEFAULTS = 360,            /* DEFAULTS  */
    DEFERRABLE = 361,          /* DEFERRABLE  */
    DEFERRED = 362,            /* DEFERRED  */
    DEFINER = 363,             /* DEFINER  */
    DELETE_P = 364,            /* DELETE_P  */
    DELIMITER = 365,           /* DELIMITER  */
    DELIMITERS = 366,          /* DELIMITERS  */
    DESC = 367,                /* DESC  */
    DICTIONARY = 368,          /* DICTIONARY  */
    DISABLE_P = 369,           /* DISABLE_P  */
    DISCARD = 370,             /* DISCARD  */
    DISTINCT = 371,            /* DISTINCT  */
    DO = 372,                  /* DO  */
    DOCUMENT_P = 373,          /* DOCUMENT_P  */
    DOMAIN_P = 374,            /* DOMAIN_P  */
    DOUBLE_P = 375,            /* DOUBLE_P  */
    DROP = 376,                /* DROP  */
    EACH = 377,                /* EACH  */
    ELSE = 378,                /* ELSE  */
    ENABLE_P = 379,            /* ENABLE_P  */
    ENCODING = 380,            /* ENCODING  */
    ENCRYPTED = 381,           /* ENCRYPTED  */
    END_P = 382,               /* END_P  */
    ENDPOINT = 383,            /* ENDPOINT  */
    ENUM_P = 384,              /* ENUM_P  */
    ESCAPE = 385,              /* ESCAPE  */
    EVENT = 386,               /* EVENT  */
    EXCEPT = 387,              /* EXCEPT  */
    EXCLUDE = 388,             /* EXCLUDE  */
    EXCLUDING = 389,           /* EXCLUDING  */
    EXCLUSIVE = 390,           /* EXCLUSIVE  */
    EXECUTE = 391,             /* EXECUTE  */
    EXISTS = 392,              /* EXISTS  */
    EXPLAIN = 393,             /* EXPLAIN  */
    EXTENSION = 394,           /* EXTENSION  */
    EXTERNAL = 395,            /* EXTERNAL  */
    EXTRACT = 396,             /* EXTRACT  */
    FALSE_P = 397,             /* FALSE_P  */
    FAMILY = 398,              /* FAMILY  */
    FETCH = 399,               /* FETCH  */
    FILTER = 400,              /* FILTER  */
    FIRST_P = 401,             /* FIRST_P  */
    FLOAT_P = 402,             /* FLOAT_P  */
    FOLLOWING = 403,           /* FOLLOWING  */
    FOR = 404,                 /* FOR  */
    FORCE = 405,               /* FORCE  */
    FOREIGN = 406,             /* FOREIGN  */
    FORWARD = 407,             /* FORWARD  */
    FREEZE = 408,              /* FREEZE  */
    FROM = 409,                /* FROM  */
    FULL = 410,                /* FULL  */
    FUNCTION = 411,            /* FUNCTION  */
    FUNCTIONS = 412,           /* FUNCTIONS  */
    GLOBAL = 413,              /* GLOBAL  */
    GRANT = 414,               /* GRANT  */
    GRANTED = 415,             /* GRANTED  */
    GREATEST = 416,            /* GREATEST  */
    GROUP_P = 417,             /* GROUP_P  */
    HANDLER = 418,             /* HANDLER  */
    HAVING = 419,              /* HAVING  */
    HEADER_P = 420,            /* HEADER_P  */
    HOLD = 421,                /* HOLD  */
    HOUR_P = 422,              /* HOUR_P  */
    IDENTITY_P = 423,          /* IDENTITY_P  */
    IF_P = 424,                /* IF_P  */
    ILIKE = 425,               /* ILIKE  */
    IMMEDIATE = 426,           /* IMMEDIATE  */
    IMMUTABLE = 427,           /* IMMUTABLE  */
    IMPLICIT_P = 428,          /* IMPLICIT_P  */
    IN_P = 429,                /* IN_P  */
    INCLUDING = 430,           /* INCLUDING  */
    INCREMENT = 431,           /* INCREMENT  */
    INDEX = 432,               /* INDEX  */
    INDEXES = 433,             /* INDEXES  */
    INHERIT = 434,             /* INHERIT  */
    INHERITS = 435,            /* INHERITS  */
    INITIALLY = 436,           /* INITIALLY  */
    INLINE_P = 437,            /* INLINE_P  */
    INNER_P = 438,             /* INNER_P  */
    INOUT = 439,               /* INOUT  */
    INPUT_P = 440,             /* INPUT_P  */
    INSENSITIVE = 441,         /* INSENSITIVE  */
    INSERT = 442,              /* INSERT  */
    INSTEAD = 443,             /* INSTEAD  */
    INT_P = 444,               /* INT_P  */
    INTEGER = 445,             /* INTEGER  */
    INTERSECT = 446,           /* INTERSECT  */
    INTERVAL = 447,            /* INTERVAL  */
    INTO = 448,                /* INTO  */
    INVOKER = 449,             /* INVOKER  */
    IS = 450,                  /* IS  */
    ISNULL = 451,              /* ISNULL  */
    ISOLATION = 452,           /* ISOLATION  */
    JOIN = 453,                /* JOIN  */
    KEY = 454,                 /* KEY  */
    LABEL = 455,               /* LABEL  */
    LANGUAGE = 456,            /* LANGUAGE  */
    LARGE_P = 457,             /* LARGE_P  */
    LAST_P = 458,              /* LAST_P  */
    LATERAL_P = 459,           /* LATERAL_P  */
    LC_COLLATE_P = 460,        /* LC_COLLATE_P  */
    LC_CTYPE_P = 461,          /* LC_CTYPE_P  */
    LEADING = 462,             /* LEADING  */
    LEAKPROOF = 463,           /* LEAKPROOF  */
    LEAST = 464,               /* LEAST  */
    LEFT = 465,                /* LEFT  */
    LEVEL = 466,               /* LEVEL  */
    LIKE = 467,                /* LIKE  */
    LIMIT = 468,               /* LIMIT  */
    LISTEN = 469,              /* LISTEN  */
    LOAD = 470,                /* LOAD  */
    LOCAL = 471,               /* LOCAL  */
    LOCALTIME = 472,           /* LOCALTIME  */
    LOCALTIMESTAMP = 473,      /* LOCALTIMESTAMP  */
    LOCATION = 474,            /* LOCATION  */
    LOCK_P = 475,              /* LOCK_P  */
    MAPPING = 476,             /* MAPPING  */
    MATCH = 477,               /* MATCH  */
    MATERIALIZED = 478,        /* MATERIALIZED  */
    MAXVALUE = 479,            /* MAXVALUE  */
    MEMORY_LIMIT = 480,        /* MEMORY_LIMIT  */
    MEMORY_SHARED_QUOTA = 481, /* MEMORY_SHARED_QUOTA  */
    MEMORY_SPILL_RATIO = 482,  /* MEMORY_SPILL_RATIO  */
    MINUTE_P = 483,            /* MINUTE_P  */
    MINVALUE = 484,            /* MINVALUE  */
    MODE = 485,                /* MODE  */
    MONTH_P = 486,             /* MONTH_P  */
    MOVE = 487,                /* MOVE  */
    NAME_P = 488,              /* NAME_P  */
    NAMES = 489,               /* NAMES  */
    NATIONAL = 490,            /* NATIONAL  */
    NATURAL = 491,             /* NATURAL  */
    NCHAR = 492,               /* NCHAR  */
    NEXT = 493,                /* NEXT  */
    NO = 494,                  /* NO  */
    NONE = 495,                /* NONE  */
    NOT = 496,                 /* NOT  */
    NOTHING = 497,             /* NOTHING  */
    NOTIFY = 498,              /* NOTIFY  */
    NOTNULL = 499,             /* NOTNULL  */
    NOWAIT = 500,              /* NOWAIT  */
    NULL_P = 501,              /* NULL_P  */
    NULLIF = 502,              /* NULLIF  */
    NULLS_P = 503,             /* NULLS_P  */
    NUMERIC = 504,             /* NUMERIC  */
    OBJECT_P = 505,            /* OBJECT_P  */
    OF = 506,                  /* OF  */
    OFF = 507,                 /* OFF  */
    OFFSET = 508,              /* OFFSET  */
    OIDS = 509,                /* OIDS  */
    ON = 510,                  /* ON  */
    ONLY = 511,                /* ONLY  */
    OPERATOR = 512,            /* OPERATOR  */
    OPTION = 513,              /* OPTION  */
    OPTIONS = 514,             /* OPTIONS  */
    OR = 515,                  /* OR  */
    ORDER = 516,               /* ORDER  */
    ORDINALITY = 517,          /* ORDINALITY  */
    OUT_P = 518,               /* OUT_P  */
    OUTER_P = 519,             /* OUTER_P  */
    OVER = 520,                /* OVER  */
    OVERLAPS = 521,            /* OVERLAPS  */
    OVERLAY = 522,             /* OVERLAY  */
    OWNED = 523,               /* OWNED  */
    OWNER = 524,               /* OWNER  */
    PARSER = 525,              /* PARSER  */
    PARTIAL = 526,             /* PARTIAL  */
    PARTITION = 527,           /* PARTITION  */
    PASSING = 528,             /* PASSING  */
    PASSWORD = 529,            /* PASSWORD  */
    PLACING = 530,             /* PLACING  */
    PLANS = 531,               /* PLANS  */
    POSITION = 532,            /* POSITION  */
    PRECEDING = 533,           /* PRECEDING  */
    PRECISION = 534,           /* PRECISION  */
    PRESERVE = 535,            /* PRESERVE  */
    PREPARE = 536,             /* PREPARE  */
    PREPARED = 537,            /* PREPARED  */
    PRIMARY = 538,             /* PRIMARY  */
    PRIOR = 539,               /* PRIOR  */
    PRIVILEGES = 540,          /* PRIVILEGES  */
    PROCEDURAL = 541,          /* PROCEDURAL  */
    PROCEDURE = 542,           /* PROCEDURE  */
    PROGRAM = 543,             /* PROGRAM  */
    QUOTE = 544,               /* QUOTE  */
    RANGE = 545,               /* RANGE  */
    READ = 546,                /* READ  */
    REAL = 547,                /* REAL  */
    REASSIGN = 548,            /* REASSIGN  */
    RECHECK = 549,             /* RECHECK  */
    RECURSIVE = 550,           /* RECURSIVE  */
    REF = 551,                 /* REF  */
    REFERENCES = 552,          /* REFERENCES  */
    REFRESH = 553,             /* REFRESH  */
    REINDEX = 554,             /* REINDEX  */
    RELATIVE_P = 555,          /* RELATIVE_P  */
    RELEASE = 556,             /* RELEASE  */
    RENAME = 557,              /* RENAME  */
    REPEATABLE = 558,          /* REPEATABLE  */
    REPLACE = 559,             /* REPLACE  */
    REPLICA = 560,             /* REPLICA  */
    RESET = 561,               /* RESET  */
    RESTART = 562,             /* RESTART  */
    RESTRICT = 563,            /* RESTRICT  */
    RETURNING = 564,           /* RETURNING  */
    RETURNS = 565,             /* RETURNS  */
    REVOKE = 566,              /* REVOKE  */
    RIGHT = 567,               /* RIGHT  */
    ROLE = 568,                /* ROLE  */
    ROLLBACK = 569,            /* ROLLBACK  */
    ROW = 570,                 /* ROW  */
    ROWS = 571,                /* ROWS  */
    RULE = 572,                /* RULE  */
    SAVEPOINT = 573,           /* SAVEPOINT  */
    SCHEMA = 574,              /* SCHEMA  */
    SCROLL = 575,              /* SCROLL  */
    SEARCH = 576,              /* SEARCH  */
    SECOND_P = 577,            /* SECOND_P  */
    SECURITY = 578,            /* SECURITY  */
    SELECT = 579,              /* SELECT  */
    SEQUENCE = 580,            /* SEQUENCE  */
    SEQUENCES = 581,           /* SEQUENCES  */
    SERIALIZABLE = 582,        /* SERIALIZABLE  */
    SERVER = 583,              /* SERVER  */
    SESSION = 584,             /* SESSION  */
    SESSION_USER = 585,        /* SESSION_USER  */
    SET = 586,                 /* SET  */
    SETOF = 587,               /* SETOF  */
    SHARE = 588,               /* SHARE  */
    SHOW = 589,                /* SHOW  */
    SIMILAR = 590,             /* SIMILAR  */
    SIMPLE = 591,              /* SIMPLE  */
    SMALLINT = 592,            /* SMALLINT  */
    SNAPSHOT = 593,            /* SNAPSHOT  */
    SOME = 594,                /* SOME  */
    STABLE = 595,              /* STABLE  */
    STANDALONE_P = 596,        /* STANDALONE_P  */
    START = 597,               /* START  */
    STATEMENT = 598,           /* STATEMENT  */
    STATISTICS = 599,          /* STATISTICS  */
    STDIN = 600,               /* STDIN  */
    STDOUT = 601,              /* STDOUT  */
    STORAGE = 602,             /* STORAGE  */
    STRICT_P = 603,            /* STRICT_P  */
    STRIP_P = 604,             /* STRIP_P  */
    SUBSTRING = 605,           /* SUBSTRING  */
    SYMMETRIC = 606,           /* SYMMETRIC  */
    SYSID = 607,               /* SYSID  */
    SYSTEM_P = 608,            /* SYSTEM_P  */
    TABLE = 609,               /* TABLE  */
    TABLES = 610,              /* TABLES  */
    TABLESPACE = 611,          /* TABLESPACE  */
    TEMP = 612,                /* TEMP  */
    TEMPLATE = 613,            /* TEMPLATE  */
    TEMPORARY = 614,           /* TEMPORARY  */
    TEXT_P = 615,              /* TEXT_P  */
    THEN = 616,                /* THEN  */
    TIME = 617,                /* TIME  */
    TIMESTAMP = 618,           /* TIMESTAMP  */
    TO = 619,                  /* TO  */
    TRAILING = 620,            /* TRAILING  */
    TRANSACTION = 621,         /* TRANSACTION  */
    TREAT = 622,               /* TREAT  */
    TRIGGER = 623,             /* TRIGGER  */
    TRIM = 624,                /* TRIM  */
    TRUE_P = 625,              /* TRUE_P  */
    TRUNCATE = 626,            /* TRUNCATE  */
    TRUSTED = 627,             /* TRUSTED  */
    TYPE_P = 628,              /* TYPE_P  */
    TYPES_P = 629,             /* TYPES_P  */
    UNBOUNDED = 630,           /* UNBOUNDED  */
    UNCOMMITTED = 631,         /* UNCOMMITTED  */
    UNENCRYPTED = 632,         /* UNENCRYPTED  */
    UNION = 633,               /* UNION  */
    UNIQUE = 634,              /* UNIQUE  */
    UNKNOWN = 635,             /* UNKNOWN  */
    UNLISTEN = 636,            /* UNLISTEN  */
    UNLOGGED = 637,            /* UNLOGGED  */
    UNTIL = 638,               /* UNTIL  */
    UPDATE = 639,              /* UPDATE  */
    USER = 640,                /* USER  */
    USING = 641,               /* USING  */
    VACUUM = 642,              /* VACUUM  */
    VALID = 643,               /* VALID  */
    VALIDATE = 644,            /* VALIDATE  */
    VALIDATOR = 645,           /* VALIDATOR  */
    VALUE_P = 646,             /* VALUE_P  */
    VALUES = 647,              /* VALUES  */
    VARCHAR = 648,             /* VARCHAR  */
    VARIADIC = 649,            /* VARIADIC  */
    VARYING = 650,             /* VARYING  */
    VERBOSE = 651,             /* VERBOSE  */
    VERSION_P = 652,           /* VERSION_P  */
    VIEW = 653,                /* VIEW  */
    VIEWS = 654,               /* VIEWS  */
    VOLATILE = 655,            /* VOLATILE  */
    WHEN = 656,                /* WHEN  */
    WHERE = 657,               /* WHERE  */
    WHITESPACE_P = 658,        /* WHITESPACE_P  */
    WINDOW = 659,              /* WINDOW  */
    WITH = 660,                /* WITH  */
    WITHIN = 661,              /* WITHIN  */
    WITHOUT = 662,             /* WITHOUT  */
    WORK = 663,                /* WORK  */
    WRAPPER = 664,             /* WRAPPER  */
    WRITE = 665,               /* WRITE  */
    XML_P = 666,               /* XML_P  */
    XMLATTRIBUTES = 667,       /* XMLATTRIBUTES  */
    XMLCONCAT = 668,           /* XMLCONCAT  */
    XMLELEMENT = 669,          /* XMLELEMENT  */
    XMLEXISTS = 670,           /* XMLEXISTS  */
    XMLFOREST = 671,           /* XMLFOREST  */
    XMLPARSE = 672,            /* XMLPARSE  */
    XMLPI = 673,               /* XMLPI  */
    XMLROOT = 674,             /* XMLROOT  */
    XMLSERIALIZE = 675,        /* XMLSERIALIZE  */
    YEAR_P = 676,              /* YEAR_P  */
    YES_P = 677,               /* YES_P  */
    ZONE = 678,                /* ZONE  */
    ACTIVE = 679,              /* ACTIVE  */
    CONTAINS = 680,            /* CONTAINS  */
    CPUSET = 681,              /* CPUSET  */
    CPU_RATE_LIMIT = 682,      /* CPU_RATE_LIMIT  */
    CREATEEXTTABLE = 683,      /* CREATEEXTTABLE  */
    CUBE = 684,                /* CUBE  */
    DECODE = 685,              /* DECODE  */
    DENY = 686,                /* DENY  */
    DISTRIBUTED = 687,         /* DISTRIBUTED  */
    DXL = 688,                 /* DXL  */
    ERRORS = 689,              /* ERRORS  */
    EVERY = 690,               /* EVERY  */
    EXCHANGE = 691,            /* EXCHANGE  */
    EXPAND = 692,              /* EXPAND  */
    FIELDS = 693,              /* FIELDS  */
    FILL = 694,                /* FILL  */
    FORMAT = 695,              /* FORMAT  */
    FULLSCAN = 696,            /* FULLSCAN  */
    GROUP_ID = 697,            /* GROUP_ID  */
    GROUPING = 698,            /* GROUPING  */
    HASH = 699,                /* HASH  */
    HOST = 700,                /* HOST  */
    IGNORE_P = 701,            /* IGNORE_P  */
    INCLUSIVE = 702,           /* INCLUSIVE  */
    INITPLAN = 703,            /* INITPLAN  */
    LIST = 704,                /* LIST  */
    LOG_P = 705,               /* LOG_P  */
    MASTER = 706,              /* MASTER  */
    MEDIAN = 707,              /* MEDIAN  */
    MISSING = 708,             /* MISSING  */
    MODIFIES = 709,            /* MODIFIES  */
    NEWLINE = 710,             /* NEWLINE  */
    NOCREATEEXTTABLE = 711,    /* NOCREATEEXTTABLE  */
    NOOVERCOMMIT = 712,        /* NOOVERCOMMIT  */
    ORDERED = 713,             /* ORDERED  */
    OTHERS = 714,              /* OTHERS  */
    OVERCOMMIT = 715,          /* OVERCOMMIT  */
    PARALLEL = 716,            /* PARALLEL  */
    RETRIEVE = 717,            /* RETRIEVE  */
    PARTITIONS = 718,          /* PARTITIONS  */
    PERCENT = 719,             /* PERCENT  */
    PERSISTENTLY = 720,        /* PERSISTENTLY  */
    PROTOCOL = 721,            /* PROTOCOL  */
    QUEUE = 722,               /* QUEUE  */
    RANDOMLY = 723,            /* RANDOMLY  */
    READABLE = 724,            /* READABLE  */
    READS = 725,               /* READS  */
    REJECT_P = 726,            /* REJECT_P  */
    REPLICATED = 727,          /* REPLICATED  */
    RESOURCE = 728,            /* RESOURCE  */
    ROLLUP = 729,              /* ROLLUP  */
    ROOTPARTITION = 730,       /* ROOTPARTITION  */
    SCATTER = 731,             /* SCATTER  */
    SEGMENT = 732,             /* SEGMENT  */
    SEGMENTS = 733,            /* SEGMENTS  */
    SETS = 734,                /* SETS  */
    SPLIT = 735,               /* SPLIT  */
    SQL = 736,                 /* SQL  */
    SUBPARTITION = 737,        /* SUBPARTITION  */
    THRESHOLD = 738,           /* THRESHOLD  */
    TIES = 739,                /* TIES  */
    VALIDATION = 740,          /* VALIDATION  */
    WEB = 741,                 /* WEB  */
    WRITABLE = 742,            /* WRITABLE  */
    YEZZEY = 743,              /* YEZZEY  */
    NULLS_FIRST = 744,         /* NULLS_FIRST  */
    NULLS_LAST = 745,          /* NULLS_LAST  */
    WITH_ORDINALITY = 746,     /* WITH_ORDINALITY  */
    WITH_TIME = 747,           /* WITH_TIME  */
    POSTFIXOP = 748,           /* POSTFIXOP  */
    UMINUS = 749               /* UMINUS  */
};
typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if !defined YYSTYPE && !defined YYSTYPE_IS_DECLARED
union YYSTYPE {
#line 174 "gram.y"

    core_YYSTYPE core_yystype;
    /* these fields must match core_YYSTYPE: */
    int ival;
    char* str;
    const char* keyword;

    char chr;
    bool boolean;
    JoinType jtype;
    DropBehavior dbehavior;
    OnCommitAction oncommit;
    List* list;
    Node* node;
    Value* value;
    ObjectType objtype;
    TypeName* typnam;
    FunctionParameter* fun_param;
    FunctionParameterMode fun_param_mode;
    FuncWithArgs* funwithargs;
    DefElem* defelt;
    SortBy* sortby;
    WindowDef* windef;
    JoinExpr* jexpr;
    IndexElem* ielem;
    Alias* alias;
    RangeVar* range;
    IntoClause* into;
    WithClause* with;
    A_Indices* aind;
    ResTarget* target;
    struct PrivTarget* privtarget;
    AccessPriv* accesspriv;
    InsertStmt* istmt;
    VariableSetStmt* vsetstmt;

#line 595 "gram.hpp"
};
typedef union YYSTYPE YYSTYPE;
#define YYSTYPE_IS_TRIVIAL 1
#define YYSTYPE_IS_DECLARED 1
#endif

/* Location type.  */
#if !defined YYLTYPE && !defined YYLTYPE_IS_DECLARED
typedef struct YYLTYPE YYLTYPE;
struct YYLTYPE {
    int first_line;
    int first_column;
    int last_line;
    int last_column;
};
#define YYLTYPE_IS_DECLARED 1
#define YYLTYPE_IS_TRIVIAL 1
#endif

int base_yyparse(std::pmr::memory_resource* resource, core_yyscan_t yyscanner);

#endif /* !YY_BASE_YY_GRAM_HPP_INCLUDED  */
