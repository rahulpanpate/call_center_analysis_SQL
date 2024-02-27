use db_call_center;

/*  ------------  Solved and unsolved Logic ---------- */
          
    /* find how much customer get solution or not*/
          
          
           select resolved,count(resolved) as total_problem_solve_unsolve
           from call_center group by resolved;
         
         
     /* find how much customer get solution or not topic wise*/
     
     
		   select resolved,topic,count(resolved) as total_problem_solve_unsolve,
           row_number() over(partition by resolved order by count(resolved) desc) as rn
           from call_center group by resolved,topic;
     
     
    /* find how much customer get solution or not agent wise*/
      
           
           select resolved,agent,count(resolved) as total_problem_solve_unsolve,
           row_number() over(partition by resolved order by count(resolved) desc) as rn
           from call_center group by resolved,agent;
          
          
     /* find how much agent solve or not solve customer issue topic wis*/ 
     
           select resolved,topic,agent,count(resolved) as total_problem_solve_unsolve,
           row_number() over(partition by resolved,topic order by count(resolved) desc) as rn
           from call_center group by resolved,topic,agent;
          
          
/*  -------- speed of answer second --------------   */ 
        
    /*how  total time take to answer for  topic wise */
      
      
        select topic,sum(speed_of_answer_in_seconds) as total_time_take
        from call_center
        group by topic;
        
   /*how  total time take to answer to solve and unsolve */
   
   
        select resolved,sum(speed_of_answer_in_seconds) as total_time_take
        from call_center
        group by resolved;
        
    /*how  total time take every agent to solve the problem */    
    
    
        select agent,sum(speed_of_answer_in_seconds) as total_time_take
        from call_center
        group by agent;
    
    /*how  total time take to answer for every topic include both y or n */
    
    
        select resolved,topic,sum(speed_of_answer_in_seconds) as total_time_take,
        row_number() over(partition by resolved order by sum(speed_of_answer_in_seconds) desc) as rn
        from call_center
        group by resolved,topic;
      
   /*how  total time take every agent to answer for every  topic wise it may be solve or unsolve */
      
      
      
        select resolved,topic,agent,sum(speed_of_answer_in_seconds) as total_time_take,
        row_number() over(partition by resolved,topic order by sum(speed_of_answer_in_seconds) desc) as rn
        from call_center
        group by resolved,topic,agent;
        
        
        
        select resolved,topic,agent,sum(speed_of_answer_in_seconds) as total_time_take,count(agent) as total_call,
        row_number() over(partition by resolved,topic order by sum(speed_of_answer_in_seconds) desc) as rn
        from call_center
        group by resolved,topic,agent;
        
        
        
        select resolved,topic,agent,sum(speed_of_answer_in_seconds) as total_time_take,count(agent) as total_call,
        sum(speed_of_answer_in_seconds)/count(agent) as average_time_per_call,
        row_number() over(partition by resolved,topic order by sum(speed_of_answer_in_seconds) desc) as rn
        from call_center
        group by resolved,topic,agent;
        
        
/* ------------  Rating  ------------- */      
           
           
            select   SatisfactionRating,count(SatisfactionRating) as Total_customer_get_rating
            from call_center
            group by SatisfactionRating order by SatisfactionRating desc;
            
            
            
            select SatisfactionRating,topic,count(SatisfactionRating) as Total_customer_get_rating
            from call_center
            group by SatisfactionRating,topic order by SatisfactionRating desc;
            
            
            
            select SatisfactionRating,topic,agent,count(SatisfactionRating),
            row_number() over(partition by SatisfactionRating,topic order by count(SatisfactionRating) desc) as rn
            from call_center
            group by SatisfactionRating,topic,agent;
            
            
            
            select SatisfactionRating,topic,agent,count(SatisfactionRating),
            row_number() over(partition by SatisfactionRating,topic order by count(SatisfactionRating) desc) as rn
            from call_center where topic="Admin Support"
            group by SatisfactionRating,topic,agent;
            
            
 select * from call_center;