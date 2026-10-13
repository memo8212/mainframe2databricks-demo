      *> ACCTREC - Account Master record layout (KSDS ACCTMAST)
      *> Copybook shared by ACCTPOST, ACCTRPT and INQTRAN.
       01  ACCT-RECORD.
           05 ACCT-NO          PIC X(10).
           05 ACCT-NAME        PIC X(30).
           05 ACCT-TYPE        PIC X(02).
           05 ACCT-BALANCE     PIC S9(11)V99 COMP-3.
           05 ACCT-STATUS      PIC X(01).
           05 ACCT-LAST-DATE   PIC 9(08).
