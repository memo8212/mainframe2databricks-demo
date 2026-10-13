       IDENTIFICATION DIVISION.
       PROGRAM-ID. ACCTPOST.
      *> Nightly account posting: apply transactions to the account master.
      *> Reads TRANS (sequential), updates ACCTMAST (VSAM KSDS, random access).
       ENVIRONMENT DIVISION.
       INPUT-OUTPUT SECTION.
       FILE-CONTROL.
           SELECT TRAN-FILE   ASSIGN TO TRANS
               ORGANIZATION IS SEQUENTIAL.
           SELECT ACCT-MASTER ASSIGN TO ACCTMAST
               ORGANIZATION IS INDEXED
               ACCESS MODE  IS RANDOM
               RECORD KEY   IS ACCT-NO
               FILE STATUS  IS WS-STATUS.
       DATA DIVISION.
       FILE SECTION.
       FD  TRAN-FILE.
       01  TRAN-REC.
           05 TR-ACCT-NO       PIC X(10).
           05 TR-TYPE          PIC X(02).
           05 TR-AMOUNT        PIC S9(9)V99 COMP-3.
       FD  ACCT-MASTER.
       COPY ACCTREC.
       WORKING-STORAGE SECTION.
       01  WS-STATUS           PIC X(02).
       01  WS-EOF              PIC X(01) VALUE 'N'.
       01  WS-INTEREST-RATE    PIC V99   VALUE 0.05.
       01  WS-INTEREST         PIC S9(11)V99 COMP-3.
       01  WS-COUNTS.
           05 WS-N-READ        PIC 9(07) VALUE 0.
           05 WS-N-POSTED      PIC 9(07) VALUE 0.
           05 WS-N-OVERDRAFT   PIC 9(07) VALUE 0.
       PROCEDURE DIVISION.
       0000-MAIN.
           OPEN INPUT  TRAN-FILE
           OPEN I-O    ACCT-MASTER
           PERFORM UNTIL WS-EOF = 'Y'
               READ TRAN-FILE
                   AT END MOVE 'Y' TO WS-EOF
                   NOT AT END
                       ADD 1 TO WS-N-READ
                       PERFORM 1000-POST-TRANSACTION
               END-READ
           END-PERFORM
           PERFORM 2000-POST-INTEREST
           CLOSE TRAN-FILE
           CLOSE ACCT-MASTER
           DISPLAY 'READ=' WS-N-READ ' POSTED=' WS-N-POSTED
                   ' OVERDRAFT=' WS-N-OVERDRAFT
           STOP RUN.

       1000-POST-TRANSACTION.
           MOVE TR-ACCT-NO TO ACCT-NO
           READ ACCT-MASTER
               INVALID KEY
                   DISPLAY 'ACCT NOT FOUND: ' TR-ACCT-NO
               NOT INVALID KEY
                   IF TR-TYPE = 'CR'
                       ADD TR-AMOUNT TO ACCT-BALANCE
                   ELSE
                       SUBTRACT TR-AMOUNT FROM ACCT-BALANCE
                   END-IF
                   IF ACCT-BALANCE < 0 AND ACCT-TYPE = 'CH'
                       MOVE 'O' TO ACCT-STATUS
                       ADD 1 TO WS-N-OVERDRAFT
                   ELSE
                       MOVE 'A' TO ACCT-STATUS
                   END-IF
                   MOVE TR-AMOUNT TO WS-INTEREST
                   REWRITE ACCT-RECORD
                   ADD 1 TO WS-N-POSTED
           END-READ.

       2000-POST-INTEREST.
      *>      Savings accounts accrue interest on positive balances.
           MOVE 'SA' TO ACCT-TYPE
           START ACCT-MASTER KEY >= ACCT-TYPE
           PERFORM UNTIL WS-STATUS NOT = '00'
               READ ACCT-MASTER NEXT
                   AT END EXIT PERFORM
                   NOT AT END
                       IF ACCT-TYPE = 'SA' AND ACCT-BALANCE > 0
                           COMPUTE WS-INTEREST =
                               ACCT-BALANCE * WS-INTEREST-RATE
                           ADD WS-INTEREST TO ACCT-BALANCE
                           REWRITE ACCT-RECORD
                       END-IF
               END-READ
           END-PERFORM.
