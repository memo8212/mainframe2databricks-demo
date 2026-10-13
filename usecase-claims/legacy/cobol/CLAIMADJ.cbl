       IDENTIFICATION DIVISION.
       PROGRAM-ID. CLAIMADJ.
      *> Claims adjudication: apply coverage, deductible and coinsurance.
      *> Reads the CLAIM file (sequential), updates MEMBMAST (VSAM KSDS).
       ENVIRONMENT DIVISION.
       INPUT-OUTPUT SECTION.
       FILE-CONTROL.
           SELECT CLAIM-FILE  ASSIGN TO CLAIMIN
               ORGANIZATION IS SEQUENTIAL.
           SELECT MEMB-MASTER ASSIGN TO MEMBMAST
               ORGANIZATION IS INDEXED
               ACCESS MODE  IS RANDOM
               RECORD KEY   IS MEMBER-ID
               FILE STATUS  IS WS-STATUS.
           SELECT ADJ-FILE    ASSIGN TO CLAIMADJ
               ORGANIZATION IS SEQUENTIAL.
       DATA DIVISION.
       FILE SECTION.
       FD  CLAIM-FILE.
       COPY CLAIMREC.
       FD  MEMB-MASTER.
       COPY MEMBREC.
       FD  ADJ-FILE.
       01  ADJ-REC.
           05 AJ-CLAIM-ID      PIC X(10).
           05 AJ-MEMBER-ID     PIC X(08).
           05 AJ-STATUS        PIC X(08).
           05 AJ-PAID-AMT      PIC S9(9)V99 COMP-3.
       WORKING-STORAGE SECTION.
       01  WS-STATUS           PIC X(02).
       01  WS-EOF              PIC X(01) VALUE 'N'.
       01  WS-COVERED          PIC X(01).
       01  WS-COINSURANCE      PIC V99.
       01  WS-ELIGIBLE         PIC S9(9)V99 COMP-3.
       01  WS-DEDUCT-APPLIED   PIC S9(9)V99 COMP-3.
       01  WS-PAID             PIC S9(9)V99 COMP-3.
       01  WS-COUNTS.
           05 WS-N-CLAIMS      PIC 9(07) VALUE 0.
           05 WS-N-DENIED      PIC 9(07) VALUE 0.
           05 WS-TOT-PAID      PIC S9(13)V99 COMP-3 VALUE 0.
       PROCEDURE DIVISION.
       0000-MAIN.
           OPEN INPUT  CLAIM-FILE
           OPEN I-O    MEMB-MASTER
           OPEN OUTPUT ADJ-FILE
           PERFORM UNTIL WS-EOF = 'Y'
               READ CLAIM-FILE
                   AT END MOVE 'Y' TO WS-EOF
                   NOT AT END
                       ADD 1 TO WS-N-CLAIMS
                       PERFORM 1000-ADJUDICATE
               END-READ
           END-PERFORM
           DISPLAY 'CLAIMS=' WS-N-CLAIMS ' DENIED=' WS-N-DENIED
                   ' TOTAL-PAID=' WS-TOT-PAID
           CLOSE CLAIM-FILE
           CLOSE MEMB-MASTER
           CLOSE ADJ-FILE
           STOP RUN.

       1000-ADJUDICATE.
           MOVE 'Y' TO WS-COVERED
           IF SERVICE-CODE = 'COS' OR SERVICE-CODE = 'ALT'
               MOVE 'N' TO WS-COVERED
           END-IF
           MOVE CLAIM-ID  TO AJ-CLAIM-ID
           MOVE MEMBER-ID TO AJ-MEMBER-ID
           IF WS-COVERED = 'N'
               MOVE 'DENIED' TO AJ-STATUS
               MOVE 0 TO AJ-PAID-AMT
               ADD 1 TO WS-N-DENIED
               WRITE ADJ-REC
               EXIT PARAGRAPH
           END-IF
      *>      look up the member and apply deductible then coinsurance
           READ MEMB-MASTER
               INVALID KEY
                   MOVE 'DENIED' TO AJ-STATUS
                   MOVE 0 TO AJ-PAID-AMT
               NOT INVALID KEY
                   EVALUATE PLAN-TYPE
                       WHEN 'GOLD'   MOVE 0.90 TO WS-COINSURANCE
                       WHEN 'SILVER' MOVE 0.80 TO WS-COINSURANCE
                       WHEN OTHER    MOVE 0.70 TO WS-COINSURANCE
                   END-EVALUATE
                   MOVE CLAIM-AMOUNT TO WS-ELIGIBLE
                   IF WS-ELIGIBLE > DEDUCTIBLE-REM
                       MOVE DEDUCTIBLE-REM TO WS-DEDUCT-APPLIED
                   ELSE
                       MOVE WS-ELIGIBLE TO WS-DEDUCT-APPLIED
                   END-IF
                   SUBTRACT WS-DEDUCT-APPLIED FROM DEDUCTIBLE-REM
                   SUBTRACT WS-DEDUCT-APPLIED FROM WS-ELIGIBLE
                   COMPUTE WS-PAID =
                       WS-ELIGIBLE * WS-COINSURANCE
                   MOVE WS-PAID TO AJ-PAID-AMT
                   IF WS-PAID > 0
                       MOVE 'APPROVED' TO AJ-STATUS
                   ELSE
                       MOVE 'NO-PAY' TO AJ-STATUS
                   END-IF
                   REWRITE MEMBER-RECORD
                   ADD WS-PAID TO WS-TOT-PAID
           END-READ
           WRITE ADJ-REC.
