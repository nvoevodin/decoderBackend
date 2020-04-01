library(data.table)
library(dplyr)
library(tools)
library(lubridate)
library(httr)
library(jsonlite)
library(RCurl)
library(curl)
library(RMySQL)
library(pool)
library(odbc)
library(RPushbullet)
library(tibble)
library(DBI)
library(RSocrata)



mydb <- dbPool(
  drv = RMySQL::MySQL(),
  dbname = "plagueDB",
  host = "67.83.57.25",
  port = 3308,
  username = "root1",
  password = "Nv0ev0din!"
)


# onStop(function() {
#   poolClose(mydb)
# })
# 
# mysqlVins <- dbGetQuery(mydb,"SELECT * from decoded")
# mysqlVins <- mysqlVins[,-1]
# mysqlVins <- mysqlVins %>% add_column(decoded = Sys.Date())
# colnames(mysqlVins) <-c('vin','make','model','year','FuelTypePrimary','FuelTypeSecondary','Type','Decoded')
# 
# pool <- dbPool(
#   RPostgres::Postgres(),
#   host = 'localhost',
#   port = 5432,
#   dbname = "cityDB",
#   user = 'postgres',
#   password = 'nv0ev0din'
# )


fhv <- fread('https://data.cityofnewyork.us/api/views/8wbx-tsch/rows.csv?accessType=DOWNLOAD') %>%
  dplyr::select(`Vehicle VIN Number`)

print('fhv success')


date <- Sys.Date()

med <- read.socrata(
  paste0("https://data.cityofnewyork.us/resource/rhe8-mgbb.json?last_updated_date=",date,""),
  app_token = "nUTPEH7ARGaLrk4pdawfZzRnK",
  email     = "voevodin.nv@gmail.com",
  password  = "Nv03041990!"
)

if(nrow(med) > 0) {

med <- med %>%
  dplyr::select(vehicle_vin_number)

colnames(med) <- 'Vehicle VIN Number'
print('med success')

vinAll <- as.data.table(bind_rows(fhv,med))

gc()

#keeping new and deleting inactive---------------------------------
#1 pull all vins from the database

dbVins <- dbGetQuery(mydb,'select * from decodedVINS')

#2 find new ones and bind them to pulled vins

newVins <- vinAll %>% filter(!`Vehicle VIN Number` %in% dbVins$vin)
colnames(newVins) <- 'vin'
dbVinsWithNew <- bind_rows(dbVins, newVins)

#3 subtract vin from pulled vins + new vins to find the ones that are inactive now

inactiveVins <- dbVinsWithNew %>% filter(!vin %in% vinAll$`Vehicle VIN Number`)

#4 delete inactive from the database

countBefore <- dbGetQuery(mydb, "SELECT COUNT(*) FROM decodedVINS")

dbGetQuery(mydb,
           paste("DELETE FROM decodedVINS 
                 WHERE vin IN ('",
                 paste(inactiveVins$vin,collapse = "','"),"')",sep = "")
)

countAfter <- dbGetQuery(mydb, "SELECT COUNT(*) FROM decodedVINS")

print(countBefore-countAfter)
#5 push the new vins through the api

if (nrow(newVins) > 0){
  ###################################################
  request <- function(x){
    r <- httr::POST(
      url = "https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/",
      body = list(
        format = "json",
        data = paste(x, collapse = ";")
      ),
      encode = "form",
      verbose()
    )
    
    results1 = jsonlite::fromJSON(httr::content(r, as = "text")) #this reads the url
    result2 = as.data.table(results1) 
    return(result2)
  }
  ########################################################
  chunk2 <- function(x,n) split(x, cut(seq_along(x), n, labels = FALSE))
  ##########################################################
  looop <- function(x){
    
    
    
    data <- request(x)
    return(data)
    
    
  }
  
  #######################################################
  
  
  if (nrow(newVins) > 15000) {
    choppedVins <- chunk2(newVins$vin,2000)
  } else if (nrow(newVins) > 2000) {
    choppedVins <- chunk2(newVins$vin,400)
  } else {
    choppedVins <- chunk2(newVins$vin,10) 
  }
  
  
  out = NULL
  for (i in choppedVins){
    data <- looop(i)[,c('Results.VIN','Results.Make','Results.Model','Results.ModelYear','Results.FuelTypePrimary','Results.FuelTypeSecondary')]
    out=rbind(out,data)
  }
  
  # activeFHV <- fread('activeFHVs.csv')
  # meds<-fread('activeMeds.csv')
  # activeFHV <- bind_rows(activeFHV,meds)
  out$Results.FuelTypePrimary[out$Results.FuelTypePrimary == ''] <- NA
  out$Results.FuelTypeSecondary[out$Results.FuelTypeSecondary == ''] <- NA
  
  tab <- c()
  tab$n <- out
  
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline' & is.na(tab$n$Results.FuelTypeSecondary),Type:='Gasoline']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline' & tab$n$Results.FuelTypeSecondary == 'Electric',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline' & tab$n$Results.FuelTypeSecondary == 'Ethanol (E85)',Type:='Hybrid']
  tab$n <- setDT(tab$n)[is.na(tab$n$Results.FuelTypePrimary) & is.na(tab$n$Results.FuelTypeSecondary),Type:='Unknown']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Flexible Fuel Vehicle (FFV)' & tab$n$Results.FuelTypeSecondary == 'Gasoline',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Flexible Fuel Vehicle (FFV)' & is.na(tab$n$Results.FuelTypeSecondary),Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Electric' & tab$n$Results.FuelTypeSecondary == 'Gasoline',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Diesel' & is.na(tab$n$Results.FuelTypeSecondary),Type:='Diesel']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline' & tab$n$Results.FuelTypeSecondary == 'Flexible Fuel Vehicle (FFV)',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Flexible Fuel Vehicle (FFV), Gasoline' & tab$n$Results.FuelTypeSecondary == 'Ethanol (E85)',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline, Flexible Fuel Vehicle (FFV)' & tab$n$Results.FuelTypeSecondary == 'Ethanol (E85)',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Electric' & is.na(tab$n$Results.FuelTypeSecondary),Type:='Electric']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline, Electric' & tab$n$Results.FuelTypeSecondary == 'Electric, Gasoline',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline' & tab$n$Results.FuelTypeSecondary == 'Compressed Natural Gas (CNG)',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Electric, Gasoline' & tab$n$Results.FuelTypeSecondary == 'Gasoline, Electric',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Gasoline, Flexible Fuel Vehicle (FFV)' & tab$n$Results.FuelTypeSecondary == 'Gasoline',Type:='Hybrid']
  tab$n <- setDT(tab$n)[is.na(tab$n$Results.FuelTypePrimary) & tab$n$Results.FuelTypeSecondary == 'Gasoline',Type:='Gasoline']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Compressed Natural Gas (CNG)' & is.na(tab$n$Results.FuelTypeSecondary),Type:='CNG']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Ethanol (E85)' & is.na(tab$n$Results.FuelTypeSecondary),Type:='Ethanol']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Flexible Fuel Vehicle (FFV)' & tab$n$Results.FuelTypeSecondary == 'Electric',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Flexible Fuel Vehicle (FFV), Gasoline' & tab$n$Results.FuelTypeSecondary == 'Gasoline',Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Electric, Gasoline' & is.na(tab$n$Results.FuelTypeSecondary),Type:='Hybrid']
  tab$n <- setDT(tab$n)[tab$n$Results.FuelTypePrimary == 'Liquefied Petroleum Gas (propane or LPG)' & is.na(tab$n$Results.FuelTypeSecondary),Type:='LPG']
  
  data <- tab$n
  
  data <- data %>% add_column(decoded = Sys.Date())
  
  
  
  colnames(data) <-c('vin','make','model','year','FuelTypePrimary','FuelTypeSecondary','Type', 'Decoded')
  data$year <- as.numeric(data$year)
  
  #create table-----------------------------------------
  #1 create empty table with correct vars
  
  # dbGetQuery(mydb,
  # "CREATE TABLE decodedVINS(
  #    vin VARCHAR(50) PRIMARY KEY,
  #    make VARCHAR(50),
  #    model VARCHAR(50),
  #    year numeric,
  #    FuelTypePrimary VARCHAR(50),
  #    FuelTypeSecondary VARCHAR(50),
  #    Type VARCHAR(50),
  #    decoded date
  # )")
  
  #2 insert the data for the first time
  
  # dbWriteTable(pool, 'decodedvins', mysqlVins, row.names=FALSE,overwrite=TRUE)
  
  #3 hash out the code
  
  #update the table-----------------------------------------
  #1 
  dbWriteTable(mydb, 'decodedVINS', dbVins, row.names=FALSE, append=TRUE)
  pbPost("note", "Decoder BackEnd Completed", paste0(nrow(data), "added, ",countBefore-countAfter," deleted." ))
  print('success!!!')
  poolClose(mydb)
} else {
  pbPost("note", "Decoder BackEnd Completed", "No new vins")
  print('no new')
  poolClose(mydb)
}

}

pbPost("note", "Decoder BackEnd Completed", "Resulted in Error")
poolClose(mydb)
##########################################################################################


# dbWriteTable(mydb, value = data, name = "decoded", overwrite = TRUE)
# 
# test1 <- dbGetQuery(mydb,"SELECT * from decoded")
# test2 <- head(test1,5000)
# 
# colnames(test2)[2] <- 'vin'
# fwrite(test2,'sample.csv')