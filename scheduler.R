  #library(taskscheduleR)
  library(cronR)
  
  ## Run every 5 minutes, starting from 10:40
  #taskscheduler_create(taskname = "pooperInsert", rscript = "upload.R",
  #                     schedule = "MINUTE", starttime = "01:35", modifier = 5)
  
  
  #f <- system.file("/home/ubuntu/server/data-loader/automate.R")
  cmd <- cron_rscript("code.R")
  cmd

  cron_add(cmd,
           frequency = '21 16 * * 2',
           at = '16:06' ,
           id = 'jon2',
          description = 'decoderBackend. Every 1 min')
