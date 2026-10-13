      *> MEMBREC - Member accumulator record layout (VSAM KSDS MEMBMAST)
       01  MEMBER-RECORD.
           05 MEMBER-ID        PIC X(08).
           05 MEMBER-NAME      PIC X(30).
           05 PLAN-TYPE        PIC X(06).
           05 DEDUCTIBLE-REM   PIC S9(7)V99 COMP-3.
           05 PLAN-YEAR        PIC 9(04).
