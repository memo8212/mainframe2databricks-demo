      *> CLAIMREC - Health claim record layout (sequential CLAIM file)
       01  CLAIM-RECORD.
           05 CLAIM-ID         PIC X(10).
           05 MEMBER-ID        PIC X(08).
           05 SERVICE-CODE     PIC X(03).
           05 CLAIM-AMOUNT     PIC S9(9)V99 COMP-3.
           05 SERVICE-DATE     PIC 9(08).
