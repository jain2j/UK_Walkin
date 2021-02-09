rm(list = ls())
gc()
library(googlesheets)
library(timeDate)
library(xtable)
library(mailR)
library(xlsx)
library(htmlTable)
library('RPostgreSQL')
library(googlesheets)
library(xtable)
library(mailR)
library(xlsx)
library(htmlTable)
library('RPostgreSQL')
library(dplyr)
library(doBy)
library(gridExtra)
library(lubridate)
library(timeDate)
library(mondate)
library(reticulate)
library(openssl)
library("RPresto")
library(botor)

drv <- dbDriver("PostgreSQL")

exp_backoff <- function(con, query) {
  exp_time = c(16,64,256,0)
  result = data.frame()
  
  for (time in exp_time) {
    result = tryCatch(
      {
        print("Executing Query")
        dbGetQuery(con, query)
      }, error = function(e) e
    )
    
    if (!inherits(result, 'error')) {
      return(result)
    }
    
    print(as.character(result))
    print(paste('Retrying your query in',time,'seconds'))
    Sys.sleep(time)
  }
  
  stop("Number of tries exceeded.")
}



library("RPresto")
con <- dbConnect(  RPresto::Presto(),  host='http://presto.oyorooms.io',  port=8889, user='revenue.analytics@oyorooms.com',schema='task_service',catalog='hive')

print('connection created')
###################################################### defining dates ###############################################
dm1 = as.Date(timeFirstDayInMonth(Sys.Date()-62))
a_date = as.POSIXlt(Sys.time())
dm2 <- as.Date(a_date) - 2
#dm1 = "2020-01-01"
#dm2 <- "2020-09-11"
################################################## SRN of properties ###############################################
srn_data <- exp_backoff( con, paste("select ss.date,
                                            ss.oyo_id, 
                                            hs.oyo_product,
                                            ss.hotel_status,
                                            (ss.srns) as srns 
                                        from
                                           (Select s.oyo_id,
                                           s.d as date, 
                                           hotel_status, 
                                           sum(s.sellable_rooms) as srns
                                           from aggregatedb.hotel_date_summary s 
                                           where s.d between date '",dm1,"' 
                                           and date '",dm2,"'  
                                           and hotel_status in (1,2,3)
                                           group by 1,2,3) ss
                                           inner join ( select oyo_id , 
                                                               oyo_product  
                                                               from aggregatedb.hotels_summary 
                                                               where oyo_product not in ('Marketplace','OYO_living', 'OYO_Living' ) 
                                                               and lower(hotel_name)  not like '%training%'
                                                               and lower(hotel_name)  not like '%test%' 
                                                               and oyo_id not like 'OYO%'  
                                                               and country_name in ('United Kingdom','Spain')
                                                               and lower(hotel_name) not like '%oyo life%')  hs
                                                               on ss.oyo_id = hs.oyo_id 
                                                                    "))
`%notin%` <- Negate(`%in%`)
srn_data <- subset(srn_data, oyo_id %notin% c("US_NYY001","US_DLS026","US_DLS027","US_VEGA001","US_DLS021","US_DLS037","US_VEGA006"))
oyo_id_list = paste("(VALUES",paste(paste0("'",unique(srn_data$oyo_id),"'"), collapse = ","),")")
############################################ Live Date ########################################################################
live_date1 = exp_backoff(con, "Select oyo_id,
                                      old_status,
                                      new_status,
                                      live_date
                              from
                                  (select a.oyo_id,
                                          a.old_status,
                                          a.new_status,
                                          date(a.time) as live_date,
                                          row_number() over (partition by a.oyo_id order by a.time desc) as row_num
                                          from aggregatedb.property_status_changes a
                                          where a.new_status='Live') as b where b.row_num=1")




first_booking<- exp_backoff(con, paste0("SELECT h.oyo_id,
                                                min(date(b.created_at)) as fb_date
                                              FROM  ordering_realtime.bookings b
                                              LEFT JOIN supply_onboarding_realtime.property h
                                              ON b.hotel_id= h.property_id
                                              WHERE h.oyo_id in ",oyo_id_list,"
                                              GROUP BY 1"))


live_date<- merge(live_date1, first_booking, by=c('oyo_id'), all=T)
live_date$live_date2<- pmin(live_date$live_date,live_date$fb_date, na.rm = T)

############################################################# Payment Policy #####################################################
pp1 <- exp_backoff(con, paste0("select  oyo_id,
                                       hotel_name,
                                       case
                                            when cast(json_extract(json_parse(payment_policy),'$.prepaid.advance') as integer) = 100 then 'Prepaid'
                                            when (cast(json_extract(json_parse(payment_policy),'$.prepaid.advance') as integer) >0 and cast(json_extract(json_parse(payment_policy),'$.prepaid.advance') as integer) < 100) then 'Partial Prepaid'
                                            when cast(json_extract(json_parse(payment_policy),'$.cash_at_hotel') as varchar) = 'true' then 'Postpaid'
                                            when cast(json_extract(json_parse(payment_policy),'$.pay_before_free_cancellation_ends') as varchar) = 'true' then 'Pay Later'
                                            when cast(json_extract(json_parse(payment_policy),'$.pay_after_free_cancellation_ends') as varchar) = 'true' then 'Pay Later'
                                            else 'Rest' end as payment_policy,
                                       case
                                            when (prepaid_restrictions is null or prepaid_restrictions ='{}') then 'Postpaid'
                                            when (case when json_extract(json_parse(prepaid_restrictions),'$.bulk_booking_advance') is not null
                                                       then
                                                            cast(case
                                                                     when REPLACE(REPLACE(cast(json_extract(json_parse(prepaid_restrictions),'$.bulk_booking_advance') as varchar), '%', ''),'\"','')=''
                                                                     then '0'
                                                                     else
                                                                          REPLACE(REPLACE(cast(json_extract(json_parse(prepaid_restrictions),'$.bulk_booking_advance') as varchar), '%', ''),'\"','')
                                                                          end as integer)
                                                        else
                                                             cast(case
                                                                       when REPLACE(REPLACE(cast(json_extract(json_parse(prepaid_restrictions),'$.advance'             ) as varchar), '%', ''),'\"','')=''
                                                                       then '0'
                                                                       else
                                                                            REPLACE(REPLACE(cast(json_extract(json_parse(prepaid_restrictions),'$.advance'             ) as varchar), '%', ''),'\"','')
                                                                       end as integer)
                                                      end) < 100
                                              then 'Partial_Prepaid'
                                              else 'Prepaid'
                                              end as prepaid_restrictions,
                                              
                                         case when prepaid_property = 'Prepaid' then 'Yes'
                                              else 'NA' end as no_postpaid_restriction
                                                  
                                 from (select distinct h.hotel_id,
                                                       oyo_id,
                                                       h.hotel_name,
                                                       hr.metadata as prepaid_restrictions,
                                                       case when hr.hotel_id is not null then 'Prepaid' else null end as prepaid_property,
                                                       case when pp.metadata is null then dpp.metadata else pp.metadata end as payment_policy
                                                        from aggregatedb.hotels_summary h
                                                        left join (select * from inventory_service.hotel_restrictions hr where  hr.status = 1
                                                                                                                              and hr.restriction_id = 12
                                                                                                                              and hr.inserted_at >='201807'
                                                                                                                              and current_date between date_add('day',hr.from_time,TIMESTAMP '1970-01-01')
                                                                                                                              and date_add('day',hr.to_time,TIMESTAMP '1970-01-01')) hr
                                                        on h.hotel_id = hr.hotel_id
                                                        left join (select * from Inventory_policies_service.default_policies dpp where dpp.policy_id = 3
                                                                                                                                      and dpp.status =1
                                                                                                                                      and dpp.inserted_at >='201807') dpp
                                                        on h.country_id = dpp.country_id
                                                        left join (select * from 
                                                                              (select pp.*, row_number() over(partition by pp.hotel_id order by pp.updated_at desc) as rank
                                                                                     from Inventory_policies_service.hotel_policies pp where pp.policy_id = 3
                                                                                                                                      and pp.status =1
                                                                                                                                      and pp.inserted_at >='201807'
                                                                                                                                      and current_date between date_add('day',pp.start_date,TIMESTAMP '1970-01-01')
                                                                                                                                      and date_add('day',pp.end_date,TIMESTAMP '1970-01-01')) pp
                                                                                                                                      where pp.rank = 1) pp
                                                        on h.hotel_id= pp.hotel_id
                                                        where h.status_id = 2
                                                        and h.country_id in (210,183)
                                                        )
                                                  
                               "))

pp1$payment_type_when_policy_applied <- ifelse(pp1$payment_policy == ('Pay Later') & pp1$no_postpaid_restriction == "Yes","Pay Later",
                                               ifelse(pp1$payment_policy == "Pay Later" & pp1$no_postpaid_restriction == "NA", pp1$prepaid_restrictions,
                                                      ifelse (pp1$payment_policy == ("Postpaid") & pp1$no_postpaid_restriction == ("Yes"), "Prepaid",
                                                              ifelse (pp1$payment_policy == ("Postpaid") & pp1$no_postpaid_restriction == ("NA"),"Postpaid",
                                                                      ifelse(pp1$payment_policy == ("Rest") & pp1$no_postpaid_restriction == ("NA"),pp1$prepaid_restrictions,
                                                                             "Prepaid")))))

pp1$payment_policy <- NULL
pp1$prepaid_restrictions <- NULL
pp1$no_postpaid_restriction <- NULL

########################################### URN BRN without channel ############################################################
inserted_at <- year(Sys.Date()-300)*100+ month(Sys.Date()-300)
urns_data <- exp_backoff(con,paste0("SELECT A.date1 as date,
                                            h.oyo_id,
                                            sum(case when B.status in (1,2) then B.oyo_rooms else 0 end) as urn,
                                            sum(case when B.status in (1,2) then (B.gmv/B.r) else 0 end) as gmv_urn,
                                            sum(case when B.status in (0,1,2,3,4,13) then B.oyo_rooms else 0 end) as brn,
                                            sum(case when B.status in (0,1,2,3,4,13) then (B.gmv/B.r) else 0 end) as gmv_brn,
                                            (sum(taxes.tax_amount/r)) as tax_amount
                                       from
                                            (SELECT DISTINCT date(cal.d) as date1 
                                              FROM default.calendar as cal
                                              WHERE date(cal.d) between date '",dm1,"' 
                                              and date '",dm2,"'
                                            )A	
                                            INNER JOIN	
                                            (	
                                            SELECT *, 
                                            (coalesce(selling_amount,amount)-coalesce(discount,0)) as gmv, 
                                            date_diff('day',cast(checkin as date),cast(checkout as date)) as r 
                                            FROM ordering_realtime.bookings 
                                                 WHERE source NOT IN (13,14) and 
                                                 status in (0,1,2,3,4,13) 
                                                 and inserted_at > '",inserted_at,"'
                                            ) as B 
                                            ON A.date1 >= date(b.checkin) 
                                            and A.date1 < date(b.checkout)
                                            inner join supply_onboarding_service.property h 
                                            on h.property_id = b.hotel_id
                                            inner join ( select oyo_id, 
                                                                country_name  from aggregatedb.hotels_summary
                                                                where oyo_product not in ('Marketplace','OYO_living', 'OYO_Living' ) 
                                                                and lower(hotel_name)  not like '%training%'
                                                                and lower(hotel_name)  not like '%test%'  
                                                                and oyo_id not like 'OYO%' 
                                                                and country_name in ('United Kingdom','Spain')
                                                                and lower(hotel_name) not like '%oyo life%' )  hs
                                                                on h.oyo_id = hs.oyo_id 
                                            LEFT JOIN (SELECT b.id,
                                                              taxes.columns['clusivity'] as clusivity,
                                                              SUM(CAST(taxes.columns['tax_amount'] as double)) as tax_amount
                                                              FROM ingestiondb.bookings_base b
                                                              inner join aggregatedb.hotels_summary hs1 
                                                              on hs1.hotel_id =  b.hotel_id
                                                              CROSS JOIN UNNEST (
                                                              CAST(
                                                              json_extract(b.taxes,'$.tax_breakup')
                                                              AS ARRAY<MAP<VARCHAR, VARCHAR>>
                                                              )) AS taxes(columns)
                                                              WHERE element_at(taxes.columns,'clusivity') is not NULL 
                                                              and taxes.columns['clusivity'] = 'Exclusive'
                                                              and b.status in (1,2)
                                                              and b.source NOT IN (13,14)
                                                              AND CAST(b.checkin AS DATE) > date '",dm1,"' 
                                                              AND CAST(b.checkin AS DATE) <= date '",dm2,"'
                                                              and hs1.country_name in ('United Kingdom','Spain')
                                                              GROUP BY 1,2) as taxes
                                            on b.id=taxes.id
                                            group by 1,2
                                            "))

urns_data[is.na(urns_data)] = 0
urns_data$urn_gmv <- urns_data$gmv_urn+urns_data$tax_amount
urns_data$tax_amount <- NULL
urns_data$gmv_urn <- NULL
check <- subset(urns_data, date >= "2020-05-01" & date <= "2020-05-24")
sum(check$urn)/24
#################################################### URN BRN with channel ###############################################
inserted_at <- year(Sys.Date()-300)*100+ month(Sys.Date()-300)
urn_contri <- exp_backoff(con,paste0("SELECT A.date1 as date, 
                                              h.oyo_id, 
                                              case when b.source in (1,10)  and b.micro_market_id is null and  C_ota.Booking_source_ota is null and b.ota_id not in (90,91,92,93,94) then 'other_ota' 
                                                   when b.source in (1,10)  and b.micro_market_id is null and b.ota_id not in (90,91,92,93,94) then  C_ota.Booking_source_ota  
                                                   when b.source in (64)  and b.micro_market_id is null then  'migration_ota'  
                                                   when  b.micro_market_id is not null and b.micro_market_id > 0 and b.micro_market_id not in (306,126,256,99) then 'MM'
                                                   when b.source in (71) and b.sub_source in (22) then 'BoG'
                                                   when b.source in (71) and b.sub_source in (23) then 'Trivago'
                                                   when b.ota_id in (90,91,92,93,94) then 'GDS'
                                                   else ce.enum_val end as booking_source,
                                              sum(case when B.status in (1,2) then B.oyo_rooms else 0 end) as urn,
                                              sum(case when B.status in (1,2) then (B.gmv/B.r) else 0 end) as gmv_urn,
                                              sum(case when B.status in (0,1,2,3,4,13) then B.oyo_rooms else 0 end) as brn,
                                              sum(case when B.status in (0,1,2,3,4,13) then (B.gmv/B.r) else 0 end) as gmv_brn,
                                              (sum(taxes.tax_amount/r)) as tax_amount
                                        from
                                             (SELECT DISTINCT date(cal.d) as date1
                                              FROM default.calendar as cal
                                              WHERE date(cal.d) between date '",dm1,"'
                                              and date '",dm2,"'
                                              )A	
                                        INNER JOIN	
                                        (	
                                            SELECT *,
                                            (coalesce(selling_amount,amount)-coalesce(discount,0)) as gmv, 
                                            date_diff('day',cast(checkin as date),cast(checkout as date)) as r
                                            FROM ordering_realtime.bookings
                                            WHERE source NOT IN (13,14) 
                                            and status in (0,1,2,3,4,13) 
                                            and inserted_at > '",inserted_at,"'
                                            ) as B 
                                            ON A.date1 >= date(b.checkin) 
                                            and A.date1 < date(b.checkout)
                                      left outer join 
                                            (Select id,case 
                                                        when name in ('Booking.com','Axis_Booking.com','Rategain_Booking.com') then 'Booking'
                                                        when name in ('Axis_Expedia','Expedia','Rategain_Expedia') then 'Expedia'
                                                        when name in ('TraveLoka','Axis_TraveLoka','Traveloka_Direct') then 'Traveloka'
                                                        when name in ('Agoda','Axis_Agoda') then 'Agoda'
                                                        when name in ('GoIbibo','Axis_Go-MMT','GoMMT','Rategain_GoIbibo','Axis_MakeMyTrip','MakeMyTrip') then 'GIMMT'
                                                        when name in ('Rakuten') then 'Rakuten'                                    
                                                        when name in ('Jalan') then 'Jalan'
                                                        when name in ('Hotel Beds') then 'Hotel Beds'
                                                        when name in ('HotelTonight') then 'HotelTonight'
                                                        when name in ('RateGain') then 'RateGain'
                                                        when name in ('RateGain_Hotwire','Hotwire') then 'Hotwire'
                                                        when name in ('RG_GetARoom') then 'GetARoom'
                                                        else 'other_ota' end as Booking_source_ota
                                            from Otadb_micro_service.online_travel_agents) C_ota
                                            ON b.ota_id = C_ota.id
                                        left outer join
                                            (select * from ingestiondb.crs_enums where table_name = 'bookings' and column_name = 'source' ) ce
                                        on ce.enum_key = b.source
                                        inner join supply_onboarding_service.property h 
                                        on h.property_id = b.hotel_id
                                        inner join ( select oyo_id, 
                                                            country_name  from aggregatedb.hotels_summary 
                                                            where oyo_product not in ('Marketplace','OYO_living', 'OYO_Living' ) 
                                                            and lower(hotel_name)  not like '%training%'
                                                            and lower(hotel_name)  not like '%test%'  
                                                            and oyo_id not like 'OYO%' 
                                                            and country_name in ('United Kingdom','Spain')
                                                            and lower(hotel_name) not like '%oyo life%' )  hs
                                                            on h.oyo_id = hs.oyo_id 
                                        LEFT JOIN (SELECT b.id,
                                                          taxes.columns['clusivity'] as clusivity,
                                                          SUM(CAST(taxes.columns['tax_amount'] as double)) as tax_amount
                                                    FROM ingestiondb.bookings_base b
                                                    inner join aggregatedb.hotels_summary hs1 
                                                          on hs1.hotel_id =  b.hotel_id 
                                                          CROSS JOIN UNNEST (
                                                                            CAST(
                                                                            json_extract(b.taxes,'$.tax_breakup')
                                                                            AS ARRAY<MAP<VARCHAR, VARCHAR>>
                                                                            )) AS taxes(columns)
                                                          WHERE element_at(taxes.columns,'clusivity') is not NULL 
                                                          and taxes.columns['clusivity'] = 'Exclusive'
                                                          and b.status in (1,2)
                                                          and b.source NOT IN (13,14)
                                                          AND CAST(b.checkout AS DATE) > date '",dm1,"' 
                                                          AND CAST(b.checkin AS DATE) <= date '",dm2,"'
                                                          and hs1.country_name in ('United Kingdom','Spain')
                                                          GROUP BY 1,2) as taxes
                                        on b.id=taxes.id
                                        group by 1,2,3
                                        "))

urn_contri[is.na(urn_contri)] = 0
urn_contri$urn_gmv <- urn_contri$gmv_urn+urn_contri$tax_amount
urn_contri$gmv_urn <- NULL
urn_contri$tax_amount <- NULL
library(reshape2)
uri_data <- recast(urn_contri,date+oyo_id~booking_source + variable,measure.var=c('urn','urn_gmv','brn','gmv_brn') )
uri_data[is.na(uri_data)]=0
print('urn done and reshaped')


############################################### realization data #############################################################
realization_data <- exp_backoff(con,paste0("SELECT A.date1 as date,
                                                   h.oyo_id, 
                                                   case when b.source in (1,10)  and b.micro_market_id is null and  C_ota.Booking_source_ota is null then 'other_ota' 
                                                        when b.source in (1,10)  and b.micro_market_id is null then  C_ota.Booking_source_ota  
                                                        when b.source in (64)  and b.micro_market_id is null then  'migration_ota'  
                                                        when  b.micro_market_id is not null and b.micro_market_id > 0 and b.micro_market_id not in (306,126,256,99) then 'MM'
                                                        when b.source in (71) and b.sub_source in (22) then 'BoG'
                                                        else ce.enum_val end as booking_source,
                                                   sum(b.oyo_rooms) as brn,
                                                   sum(case when b.status in (1,2)  then b.oyo_rooms else 0 end) as urn,
                                                   ((sum(case when
                                                   date(date_add('minute', tzb.timeint, cast(substr((case when coalesce(b.cancellation_time,b.updated_at) <> 'null' 
                                                   then coalesce(b.cancellation_time,b.updated_at) else '2000-01-01 01:01:01' end),1,19) 
                                                   as TIMESTAMP))) = date(b.checkin) and 
        
                                                   extract(hour from (date_add('minute', tzb.timeint, cast(substr((case when coalesce(b.cancellation_time,b.updated_at) <> 'null' 
                                                   then coalesce(b.cancellation_time,b.updated_at) else '2000-01-01 01:01:01' end),1,19) 
                                                   as TIMESTAMP)))) >= 18 and b.status =  3 then b.oyo_rooms else 0 end)) +  
        
                                                    (sum(case when
                                                   date(date_add('minute', tzb.timeint, cast(substr((case when coalesce(b.cancellation_time,b.updated_at) <> 'null' 
                                                   then coalesce(b.cancellation_time,b.updated_at) else '2000-01-01 01:01:01' end),1,19) 
                                                   as TIMESTAMP))) > date(b.checkin) and 
                                                    b.status =  3 then b.oyo_rooms else 0 end))) as bad_canc,
                                                   sum(case when b.status = 3 then b.oyo_rooms else 0 end) as cancellations,
                                                   sum(case when b.status = 4 then b.oyo_rooms else 0 end) as no_show,
                                                   sum(case when b.status = 0 then b.oyo_rooms else 0 end) as confirmed,
                                                   sum(case when b.status = 13 then b.oyo_rooms else 0 end) as void
                                           from
                                           (SELECT DISTINCT date(cal.d) as date1 FROM default.calendar as cal
                                           WHERE date(cal.d) between date '",dm1,"' and date '",dm2,"'
                                           )A	
                                           INNER JOIN	
                                           (	
                                           SELECT *, 
                                           date_diff('day',cast(checkin as date),cast(checkout as date)) as r FROM ingestiondb.bookings_base 
                                           WHERE source NOT IN (13,14) and status in (0,1,2,3,4,13) and inserted_at > '",inserted_at,"'
                                           ) as B 
                                           ON A.date1 >= date(b.checkin) and A.date1 < date(b.checkout)
                                           left outer join 
                                           (Select id,case 
                                           when name in ('Booking.com','Axis_Booking.com','Rategain_Booking.com') then 'Booking'
                                           when name in ('Axis_Expedia','Expedia','Rategain_Expedia') then 'Expedia'
                                           when name in ('TraveLoka','Axis_TraveLoka','Traveloka_Direct') then 'Traveloka'
                                           when name in ('Agoda','Axis_Agoda') then 'Agoda'
                                           when name in ('GoIbibo','Axis_Go-MMT','GoMMT','Rategain_GoIbibo','Axis_MakeMyTrip','MakeMyTrip') then 'GIMMT'
                                           when name in ('Rakuten') then 'Rakuten'                                    
                                           when name in ('Jalan') then 'Jalan'
                                           when name in ('Hotel Beds') then 'Hotel Beds'
                                           when name in ('HotelTonight') then 'HotelTonight'
                                           when name in ('RateGain') then 'RateGain'
                                           when name in ('RateGain_Hotwire','Hotwire') then 'Hotwire'
                                           when name in ('RG_GetARoom') then 'GetARoom'
                                           else 'other_ota' end as Booking_source_ota
                                           from Otadb_micro_service.online_travel_agents) C_ota
                                           ON b.ota_id = C_ota.id
                                           left outer join
                                           (select * from ingestiondb.crs_enums where table_name = 'bookings' and column_name = 'source' ) ce
                                           on ce.enum_key = b.source
                                           inner join supply_onboarding_service.property h on h.property_id = b.hotel_id
                                           inner join ( select oyo_id, country_name  from aggregatedb.hotels_summary where oyo_product 
                                           not in ('Marketplace','OYO_living', 'OYO_Living' ) and lower(hotel_name)  not like '%training%'
                                           and lower(hotel_name)  not like '%test%'  and oyo_id not like 'OYO%' and country_name in ('United Kingdom','Spain')
                                           and lower(hotel_name) not like '%oyo life%' )  hs
                                           on h.oyo_id = hs.oyo_id 
                                           left outer join supply_discovery_service.clusters c on c.id = h.cluster_id
                                           left outer join supply_discovery_service.cities ct on ct.id = c.city_id
                                           left outer join (select tz.name,
                                           case when substr(tz.offset,1,1) = '+' then 1*(cast(substr(tz.offset,2,2) as integer)*60 +cast(substr(tz.offset,5,2) as integer))
                                           else -1*(cast(substr(tz.offset,2,2) as integer)*60 +cast(substr(tz.offset,5,2) as integer)) end as timeint
                                           from ingestiondb.city_tz tz) tzb on tzb.name = ct.time_zone
                                           group by 1,2,3
                                           "))

canc_data <- recast(realization_data,date+oyo_id~booking_source + variable,measure.var=c('cancellations','no_show'))
canc_data[is.na(canc_data)] <- 0

########################################### Properties extraction #####################################################
reference <- exp_backoff(con,paste0("select oyo_id,
                                            hotel_name,
                                            city_name,
                                            zone_name
                                            from aggregatedb.hotels_summary 
                                            where country_name in ('United Kingdom','Spain')
                                            and oyo_product not in ('Marketplace','OYO_living', 'OYO_Living' ) 
                                            and lower(hotel_name)  not like '%training%'
                                            and lower(hotel_name)  not like '%test%' 
                                            and oyo_id not like 'OYO%'"))

prop <- subset(reference, reference$oyo_id %in% srn_data$oyo_id)
################################################### currrent floor #################################################

######################################################### contracted room ####################################################
cr <- exp_backoff( con, paste("select ss.d as date,
                                      ss.oyo_id, 
                                     (ss.total_contracted_rooms) as total_contracted_rooms 
                                          from
                                               (Select s.d,s.oyo_id,  max(s.total_contracted_rooms) as total_contracted_rooms
                                               from aggregatedb.hotel_date_summary s where
                                               s.d between date '",dm1,"' and date '",dm2,"'  
                                               group by 1,2) ss
                                               inner join ( select oyo_id , oyo_product  from aggregatedb.hotels_summary where oyo_product 
                                               not in ('Marketplace','OYO_living', 'OYO_Living' ) and lower(hotel_name)  not like '%training%'
                                               and lower(hotel_name)  not like '%test%' and oyo_id not like 'OYO%'  and country_name in ('United Kingdom','Spain')
                                               and lower(hotel_name) not like '%oyo life%'   )  hs
                                               on ss.oyo_id = hs.oyo_id 
                                               "))

cr <- subset(cr, cr$oyo_id %in% srn_data$oyo_id)
####################################################### Merging Database and arranging them ####################################
merge <- merge(urns_data, srn_data, by = c("date","oyo_id"), all.y = T)
merge[is.na(merge)] <- 0
merge$oyo_product <- NULL
srn_urn1 <- merge
srn_urn1$srns <- ifelse(srn_urn1$hotel_status == 2, srn_urn1$srns, ifelse(srn_urn1$hotel_status != 2 & srn_urn1$urn >0 , srn_urn1$urn , 0))
#srn_urn1 <- subset(srn_urn1, srn_urn1$srns + srn_urn1$urn >0)
srn_urn1$srns <- ifelse(srn_urn1$srns == 0 & srn_urn1$urn > 0, srn_urn1$urn, srn_urn1$srns)

merge <- srn_urn1

merge1 <- merge(merge, uri_data, by = c("date","oyo_id"), all.x = T)
merge2 <- merge(merge1, canc_data, by = c("date","oyo_id"), all.x = T)

#merge2$OTA_urn <- merge2$Agoda_urn+merge2$Booking_urn+merge2$Expedia_urn+merge2$GetARoom_urn+merge2$`Hotel Beds_urn`+merge2$Hotwire_urn+merge2$HotelTonight_urn+merge2$RateGain_urn+merge2$other_ota_urn
#merge2$OTA_brn <- merge2$Agoda_brn+merge2$Booking_brn+merge2$Expedia_brn+merge2$GetARoom_brn+merge2$`Hotel Beds_brn`+merge2$Hotwire_brn+merge2$HotelTonight_brn+merge2$RateGain_brn+merge2$other_ota_brn
#merge2$OTA_gmv_urn <- merge2$Agoda_urn_gmv+merge2$Booking_urn_gmv+merge2$Expedia_urn_gmv+merge2$GetARoom_urn_gmv+merge2$`Hotel Beds_urn_gmv`+merge2$Hotwire_urn_gmv+merge2$HotelTonight_urn_gmv+merge2$RateGain_urn_gmv+merge2$other_ota_urn_gmv
#merge2$OTA_gmv_brn <- merge2$Agoda_gmv_brn+merge2$Booking_gmv_brn+merge2$Expedia_gmv_brn+merge2$GetARoom_gmv_brn+merge2$`Hotel Beds_gmv_brn`+merge2$Hotwire_gmv_brn+merge2$HotelTonight_gmv_brn+merge2$RateGain_gmv_brn+merge2$other_ota_gmv_brn
#merge2$OTA_noshows <- merge2$Agoda_no_show+merge2$Booking_no_show+merge2$Expedia_no_show+merge2$GetARoom_no_show+merge2$`Hotel Beds_no_show`+merge2$Hotwire_no_show+merge2$HotelTonight_no_show+merge2$RateGain_no_show+merge2$other_ota_no_show
#merge2$OTA_cancellations <- merge2$Agoda_cancellations+merge2$Booking_cancellations+merge2$Expedia_cancellations+merge2$GetARoom_cancellations+merge2$`Hotel Beds_cancellations`+merge2$Hotwire_cancellations+merge2$HotelTonight_cancellations+merge2$RateGain_cancellations+merge2$other_ota_cancellations

##### New Logic

merge2$OTA_urn           <- rowSums(merge2[,intersect(c(paste0(unique(urn_contri$booking_source),'_urn'          )),c('Agoda_urn',          'Booking_urn',          'Expedia_urn',          'GetARoom_urn',          'Hotel Beds_urn',          'Hotwire_urn',          'HotelTonight_urn',          'RateGain_urn',          'other_ota_urn'          ))],na.rm=TRUE)
merge2$OTA_brn           <- rowSums(merge2[,intersect(c(paste0(unique(urn_contri$booking_source),'_brn'          )),c('Agoda_brn',          'Booking_brn',          'Expedia_brn',          'GetARoom_brn',          'Hotel Beds_brn',          'Hotwire_brn',          'HotelTonight_brn',          'RateGain_brn',          'other_ota_brn'          ))],na.rm=TRUE)
merge2$OTA_gmv_urn       <- rowSums(merge2[,intersect(c(paste0(unique(urn_contri$booking_source),'_urn_gmv'      )),c('Agoda_urn_gmv',      'Booking_urn_gmv',      'Expedia_urn_gmv',      'GetARoom_urn_gmv',      'Hotel Beds_urn_gmv',      'Hotwire_urn_gmv',      'HotelTonight_urn_gmv',      'RateGain_urn_gmv',      'other_ota_urn_gmv'      ))],na.rm=TRUE)
merge2$OTA_gmv_brn       <- rowSums(merge2[,intersect(c(paste0(unique(urn_contri$booking_source),'_gmv_brn'      )),c('Agoda_gmv_brn',      'Booking_gmv_brn',      'Expedia_gmv_brn',      'GetARoom_gmv_brn',      'Hotel Beds_gmv_brn',      'Hotwire_gmv_brn',      'HotelTonight_gmv_brn',      'RateGain_gmv_brn',      'other_ota_gmv_brn'      ))],na.rm=TRUE)
merge2$OTA_noshows       <- rowSums(merge2[,intersect(c(paste0(unique(urn_contri$booking_source),'_no_show'      )),c('Agoda_no_show',      'Booking_no_show',      'Expedia_no_show',      'GetARoom_no_show',      'Hotel Beds_no_show',      'Hotwire_no_show',      'HotelTonight_no_show',      'RateGain_no_show',      'other_ota_no_show'      ))],na.rm=TRUE)
merge2$OTA_cancellations <- rowSums(merge2[,intersect(c(paste0(unique(urn_contri$booking_source),'_cancellations')),c('Agoda_cancellations','Booking_cancellations','Expedia_cancellations','GetARoom_cancellations','Hotel Beds_cancellations','Hotwire_cancellations','HotelTonight_cancellations','RateGain_cancellations','other_ota_cancellations'))],na.rm=TRUE)


merge2$Growth_urn <- merge2$`Android App_urn`+merge2$`IOS App_urn`+merge2$`Mobile Web Booking_urn`+merge2$`Web Booking_urn`+merge2$Direct_urn
merge2$Growth_brn <- merge2$`Android App_brn`+merge2$`IOS App_brn`+merge2$`Mobile Web Booking_brn`+merge2$`Web Booking_brn`+merge2$Direct_brn
merge2$Growth_gmv_urn <- merge2$`Android App_urn_gmv`+merge2$`IOS App_urn_gmv`+merge2$`Mobile Web Booking_urn_gmv`+merge2$`Web Booking_urn_gmv`+merge2$Direct_urn_gmv
merge2$Growth_gmv_brn <- merge2$`Android App_gmv_brn`+merge2$`IOS App_gmv_brn`+merge2$`Mobile Web Booking_gmv_brn`+merge2$`Web Booking_gmv_brn`+merge2$Direct_gmv_brn
merge2$Growth_noshows <- merge2$`Android App_no_show`+merge2$`IOS App_no_show`+merge2$`Mobile Web Booking_no_show`+merge2$`Web Booking_no_show`+merge2$Direct_no_show
merge2$Growth_cancellations <- merge2$`Android App_cancellations`+merge2$`IOS App_cancellations`+merge2$`Mobile Web Booking_cancellations`+merge2$`Web Booking_cancellations`+merge2$Direct_cancellations

merge3 <- merge(merge2, cr, by = c("date","oyo_id"), all.x = T)
merge4 <- merge(merge3, prop, by = "oyo_id", all.x = T)
merge4[is.na(merge4)] <- 0

merge5 <- merge(merge4, pp1[,c("oyo_id","payment_type_when_policy_applied")], by = "oyo_id", all.x = T)
merge6 <- merge(merge5, live_date[,c("oyo_id","live_date2")], by = "oyo_id", all.x = T)

merge6$current_crs_status <- case_when(merge6$hotel_status == 0 ~ "On Hold",
                                       merge6$hotel_status == 1 ~ "Active",
                                       merge6$hotel_status == 2 ~ "Live",
                                       merge6$hotel_status == 3 ~ "Blocked",
                                       merge6$hotel_status == 4 ~ "In Progress",
                                       merge6$hotel_status == 5 ~ "Training")
merge6$hotel_status <- NULL

merge7=merge6

merge8= select(merge7,1,2,3,4,5,6,7,64,65,66,67,98,99,114,115,116,117,118,119,120)


write.csv(merge6,'360_file_UK_Spain_29_Jan.csv',row.names = F)
write.csv(merge8,'360_file_UK_Spain_29_Jan_1.csv',row.names = F)
print('csv downloaded')


getwd()

s3_upload_file('360_file_UK_Spain_29_Jan_1.csv','s3://prod-datapl-r-scheduler/team/usa_revenue_analytics/mayank.shinde/prepayment_impact_analysis/360_file_UK_Spain_29_Jan_1.csv')
print('csv downloaded')

source_python('./UK_Walkin/dashboard_writer.py')
