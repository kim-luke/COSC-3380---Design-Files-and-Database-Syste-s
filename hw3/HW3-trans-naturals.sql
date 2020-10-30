CREATE TABLE T_trans(i int); /* naturals */
CREATE TABLE C_trans(i int); /* counts */
INSERT INTO C_trans VALUES(0);

/* 
iterate this transaction
keep a counter of naturals inserted 
*/
START TRANSACTION;

 INSERT INTO T_trans
 SELECT C_trans.i+1
 FROM C_trans;

 UPDATE C_trans
 SET i=i+1;
COMMIT;


