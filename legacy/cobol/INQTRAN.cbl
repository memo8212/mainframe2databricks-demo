       IDENTIFICATION DIVISION.
       PROGRAM-ID. INQTRAN.
      *> CICS online transaction: balance inquiry by account number.
      *> Reads ACCTMAST (VSAM KSDS, random) and returns balance to the screen.
       ENVIRONMENT DIVISION.
       DATA DIVISION.
       WORKING-STORAGE SECTION.
       01  WS-COMMAREA.
           05 CA-ACCT-NO       PIC X(10).
           05 CA-BALANCE       PIC S9(11)V99 COMP-3.
           05 CA-STATUS        PIC X(01).
           05 CA-RESP-CODE     PIC X(02).
       LINKAGE SECTION.
       01  DFHCOMMAREA.
           05 DFH-ACCT-NO      PIC X(10).
       PROCEDURE DIVISION.
       0000-MAIN.
           MOVE DFH-ACCT-NO TO ACCT-NO
           EXEC CICS READ
               FILE('ACCTMAST')
               INTO(ACCT-RECORD)
               RIDFLD(ACCT-NO)
               RESP(CA-RESP-CODE)
           END-EXEC
           IF CA-RESP-CODE = '00'
               MOVE ACCT-BALANCE TO CA-BALANCE
               MOVE ACCT-STATUS  TO CA-STATUS
               EXEC CICS SEND MAP('INQMAP') FROM(WS-COMMAREA) END-EXEC
           ELSE
               EXEC CICS SEND TEXT('ACCOUNT NOT FOUND') END-EXEC
           END-IF
           EXEC CICS RETURN END-EXEC.
