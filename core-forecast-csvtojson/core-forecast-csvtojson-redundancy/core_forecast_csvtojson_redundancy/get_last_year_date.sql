select    CASE 
                      WHEN ( 
                                            Year('%s') % 4 = 0 
                                 AND        Year('%s') % 100 != 0)   and '%s' >concat(year('%s'),'-02-28')
                      OR         Year('%s') % 400 = 0 THEN Date_add(Date_add('%s', INTERVAL - 1 year), INTERVAL + 2 day) 
                   
              WHEN ( 
                                            (Year('%s')-1) % 4 = 0
                                 AND        (Year('%s')-1) % 100 != 0)    and '%s' <=concat( (Year('%s')),'-02-28')
                                 OR         (Year('%s')-1) % 400 = 0
                       THEN Date_add(Date_add('%s', INTERVAL - 1 year), INTERVAL + 2 day) 
                     
         ELSE Date_add(Date_add('%s', INTERVAL                                      - 1 year), INTERVAL + 1 day)    
           end