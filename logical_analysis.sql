use db_call_center;

/* ---------- every topic wise highest  call attend agent name  ------- */
		  
          select topic,agent,total_call from (
          select topic,agent,count(agent) as total_call,
          row_number() over(partition by topic order by count(agent) desc) as rn
          from call_center
          group by topic,agent) m where m.rn=1 order by total_call desc;
          
          /*  above result verify below query pls check */
          
             select topic,agent,count(agent),
             row_number() over(partition by topic order by count(agent) desc) as rn
             from call_center
             group by topic,agent;
             
  
/*--------- find every hour and every topic which agent do highest call-----------*/


            select  hr as "o'clock",topic,agent,total_call from (
            select hour(time) as hr,topic,agent,count(agent) as total_call,
            row_number() over(partition by  hour(time),topic order by count(agent) desc ) as rn
            from call_center 
            group by hour(time),topic,agent) p where p.rn=1;  
            
             
           /*  verify above result in below query  */  
           
                 select hour(time) as "o'clock",topic,agent,count(agent) as total_call,
                 row_number() over(partition by  hour(time),topic order by count(agent) desc ) as rn
                 from call_center
                 group by hour(time),topic,agent;
                 
             /* i want show only admin support  who get highest call in every hour*/   
                    
                    select hr as "o'clcok",topic,agent,total_call from (
                    select hour(time) as hr,topic,agent,count(agent) as total_call,
                    row_number() over(partition by  hour(time),topic order by count(agent) desc ) as rn
                    from call_center where topic = "Admin Support"
                    group by hour(time),topic,agent) z where z.rn=1;
                    
                    /* verify above result in below code */
                    
                    select hour(time) as "o'clock",topic,agent,count(agent) as total_call,
                    row_number() over(partition by  hour(time),topic order by count(agent) desc ) as rn
                    from call_center where topic = "Admin Support"
                    group by hour(time),topic,agent;
                 



select * from call_center;