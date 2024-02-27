/* 
     NAME  -  RAHUL PANPATE
     LINKEDIN LINK  - www.linkedin.com/in/rahulpanpate
*/
create database db_call_center;

use db_call_center;



/*  --- 	FIND TOTAL  RECORD IN CALL CENTER TABLE  ---- */

            SELECT COUNT(CALL_ID) AS TOTAL_RECORD FROM CALL_CENTER;
            
/*  --- 	FIND TOTAL  AGENT WORK  IN CALL CENTER TABLE  ---- */

            SELECT COUNT(DISTINCT(AGENT)) AS TOTAL_AGENT FROM CALL_CENTER;
            
/*  --- 	FIND TOTAL  TOPIC   IN CALL CENTER TABLE  ---- */

            SELECT COUNT(DISTINCT(TOPIC)) AS TOTAL_AGENT FROM CALL_CENTER;
            
/*  --- 	FIND TOTAL CALL ATTEND AGEND WISE  ---- */

            SELECT AGENT, COUNT(AGENT) AS TOTAL_CALL FROM CALL_CENTER
            GROUP BY AGENT ORDER BY TOTAL_CALL DESC;			
 
/*  --- 	FIND TOTAL  CALL   IN TOPIC WISE  ---- */

            SELECT TOPIC, COUNT(TOPIC) AS TOTAL_CALL FROM CALL_CENTER
            GROUP BY TOPIC ORDER BY TOTAL_CALL  DESC ; 
            
/*  --- 	FIND HOW MANY PROBLEM SOLVE AND UNSOLVE  ---- */

            SELECT RESOLVED, COUNT(RESOLVED) AS TOTAL_CALL FROM CALL_CENTER
            GROUP BY RESOLVED ORDER BY TOTAL_CALL  DESC ;     
            
            
			SET SQL_SAFE_UPDATES = 0;
            UPDATE CALL_CENTER SET CALL_DATE = STR_TO_DATE(CALL_DATE, '%d/%m/%Y');
            
           
/*  --- 	FIND DAY WISE TOTAL CALL  ---- */

            SELECT DAYNAME(CALL_DATE) AS DAY, COUNT(DAYNAME(CALL_DATE)) AS TOTAL_CALL FROM CALL_CENTER
            GROUP BY DAYNAME(CALL_DATE) ORDER BY TOTAL_CALL  DESC ;               

/*  --- 	FIND TOTAL CALL ATTEND BY AGENT TOPIC WISE  ---- */

            SELECT AGENT,TOPIC,COUNT(TOPIC) AS TOTAL_CALL,
            ROW_NUMBER() OVER(PARTITION BY AGENT ORDER BY COUNT(TOPIC) DESC )
            FROM CALL_CENTER 
            GROUP BY AGENT,TOPIC;
            
/*  --- 	FIND AGENT WISE RESOLVE CALL  ---- */

            SELECT AGENT,COUNT(RESOLVED) AS TOTAL_CALL_RESOLVED
            FROM CALL_CENTER 
            WHERE RESOLVED="Y"
            GROUP BY AGENT ORDER BY TOTAL_CALL_RESOLVED DESC;   

/*  --- 	FIND AGENT WISE UNSOLVE CALL  ---- */

            SELECT AGENT,COUNT(RESOLVED) AS TOTAL_CALL_UNSOLVED
            FROM CALL_CENTER 
            WHERE RESOLVED="N"
            GROUP BY AGENT ORDER BY TOTAL_CALL_UNSOLVED DESC;
       
/*  --- 	FIND TOPIC WISE RESOLVE CALL  ---- */

            SELECT TOPIC,COUNT(RESOLVED) AS TOTAL_CALL_RESOLVED
            FROM CALL_CENTER 
            WHERE RESOLVED="Y"
            GROUP BY TOPIC ORDER BY TOTAL_CALL_RESOLVED DESC;    

/*  --- 	FIND TOPIC WISE UNSOLVE CALL  ---- */

            SELECT TOPIC,COUNT(RESOLVED) AS TOTAL_CALL_UNSOLVED
            FROM CALL_CENTER 
            WHERE RESOLVED="N"
            GROUP BY TOPIC ORDER BY TOTAL_CALL_UNSOLVED DESC; 
            
/*  --- 	O'CLOCK WISE TOTAL  CALL  ---- */ 

            SELECT hour(TIME) AS "O'CLOCK",COUNT(TIME) AS TOTAL_CALL
            FROM CALL_CENTER
            GROUP BY hour(TIME);
            
            /*  HOUR WISE AGENT ATTEND TOTAL CALL  */
            
                 SELECT hour(TIME) AS "O'CLOCK",AGENT,COUNT(TIME) AS TOTAL_CALL ,
                 ROW_NUMBER() OVER(PARTITION BY hour(TIME) ORDER BY COUNT(TIME) DESC) AS RN
                 FROM CALL_CENTER
                 GROUP BY hour(TIME) ,AGENT ;
            
            /*---EVERY HOUR WHICH AGENT GET HIGHEST CALL ATTEND----*/
                  
				 SELECT HR AS "O'CLOCK",AGENT AS MOST_CALL_ATTEND_HOUR_WISE_NAME,TOTAL_CALL FROM
                 (SELECT hour(TIME) AS HR,AGENT,COUNT(TIME) AS TOTAL_CALL ,
                 ROW_NUMBER() OVER(PARTITION BY hour(TIME) ORDER BY COUNT(TIME) DESC) AS RN
                 FROM CALL_CENTER
                 GROUP BY hour(TIME) ,AGENT) T WHERE T.RN=1 ;
                 
                 
              /*  EVERY HOUR TOTAL CALL BY TOPIC WISE  */   
                  
                 
                  
                  SELECT HOUR(TIME) AS "O'CLOCK",TOPIC,COUNT(TOPIC) AS TOTAL_CALL,
                  row_number() OVER(partition by HOUR(TIME) ORDER BY COUNT(TOPIC) DESC) AS RN
                  FROM CALL_CENTER
                  GROUP BY HOUR(TIME),TOPIC;
              
              /*---EVERY HOUR WHICH TOPICC COME HIGHEST CALL ----*/
                    
                    SELECT HR AS "O'CLOCK",TOPIC,TOTAL_CALL FROM
                    (SELECT HOUR(TIME) AS HR,TOPIC,COUNT(TOPIC) AS TOTAL_CALL,
                    row_number() OVER(partition by HOUR(TIME) ORDER BY COUNT(TOPIC) DESC) AS RN
                    FROM CALL_CENTER
					GROUP BY HOUR(TIME),TOPIC) X WHERE X.RN=1;
                    
                    
/* -------- RATING ------ */   
           
               
               SELECT AGENT,SatisfactionRating,COUNT(SatisfactionRating) TOTAL_CUSTOMER_GIVE_RATING,
               row_number() OVER(partition by AGENT order by COUNT(SatisfactionRating) DESC) AS RN
               FROM CALL_CENTER
               GROUP BY AGENT,SatisfactionRating;
 
 select * from call_center;
 

