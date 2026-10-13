//CLAIMJOB JOB (HLTH),'NIGHTLY CLAIMS ADJ',CLASS=A,MSGCLASS=X
//* ------------------------------------------------------------------
//* Nightly claims adjudication batch.
//*   STEP010  sort claims by claim id (process order)
//*   STEP020  adjudicate claims against member accumulators (VSAM)
//*   STEP030  claims report (paid totals, denial rate)
//* ------------------------------------------------------------------
//STEP010  EXEC PGM=SORT
//SYSIN    DD *
  SORT FIELDS=(1,10,CH,A)
/*
//STEP020  EXEC PGM=CLAIMADJ,COND=(0,NE,STEP010)
//CLAIMIN  DD DSN=HLTH.CLAIM.DAILY,DISP=SHR
//MEMBMAST DD DSN=HLTH.VSAM.MEMBMAST,DISP=SHR
//CLAIMADJ DD DSN=HLTH.CLAIM.ADJUDICATED,DISP=(NEW,CATLG,DELETE)
//SYSOUT   DD SYSOUT=*
//STEP030  EXEC PGM=CLAIMRPT,COND=(0,NE,STEP020)
//CLAIMADJ DD DSN=HLTH.CLAIM.ADJUDICATED,DISP=SHR
//SYSOUT   DD SYSOUT=*
//
