       IDENTIFICATION DIVISION.
       PROGRAM-ID. ACCTRPT.
      *> End-of-night report: totals by account type, list overdrawn accounts.
       ENVIRONMENT DIVISION.
       INPUT-OUTPUT SECTION.
       FILE-CONTROL.
           SELECT ACCT-MASTER ASSIGN TO ACCTMAST
               ORGANIZATION IS INDEXED
               ACCESS MODE  IS SEQUENTIAL
               RECORD KEY   IS ACCT-NO.
           SELECT RPT-FILE    ASSIGN TO ACCTRPT
               ORGANIZATION IS SEQUENTIAL.
       DATA DIVISION.
       FILE SECTION.
       FD  ACCT-MASTER.
       COPY ACCTREC.
       FD  RPT-FILE.
       01  RPT-LINE            PIC X(80).
       WORKING-STORAGE SECTION.
       01  WS-EOF              PIC X(01) VALUE 'N'.
       01  WS-TOT-BAL          PIC S9(13)V99 COMP-3 VALUE 0.
       01  WS-N-ACCT           PIC 9(07) VALUE 0.
       PROCEDURE DIVISION.
       0000-MAIN.
           OPEN INPUT ACCT-MASTER
           OPEN OUTPUT RPT-FILE
           PERFORM UNTIL WS-EOF = 'Y'
               READ ACCT-MASTER
                   AT END MOVE 'Y' TO WS-EOF
                   NOT AT END
                       ADD 1 TO WS-N-ACCT
                       ADD ACCT-BALANCE TO WS-TOT-BAL
                       IF ACCT-STATUS = 'O'
                           DISPLAY 'OVERDRAWN ' ACCT-NO ' ' ACCT-NAME
                       END-IF
               END-READ
           END-PERFORM
           DISPLAY 'ACCOUNTS=' WS-N-ACCT ' TOTAL-BAL=' WS-TOT-BAL
           CLOSE ACCT-MASTER
           CLOSE RPT-FILE
           STOP RUN.
