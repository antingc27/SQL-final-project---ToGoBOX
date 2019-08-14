install.packages('RPostgreSQL')
install.packages('dplyr')
install.packages('tidyr')

require('RPostgreSQL')
require('dplyr')
require('tidyr')

drv <- dbDriver('PostgreSQL')

con <- dbConnect(drv, dbname = 'final1',
                 host = 'su19server.apan5310.com', port = 50203,
                 user = 'postgres', password = 'rjxklxet')


sql <- "

CREATE TABLE vendor (
vendor_id char(10), 
vendor varchar(30), 
vendor_create_date timestamp, 
PRIMARY KEY(vendor_id)
);

CREATE TABLE vendor_contact (
vendor_contact_id char(10), 
vendor_id char(10),
vendor_phone varchar(30), 
vendor_website varchar(50), 
PRIMARY KEY(vendor_contact_id), 
FOREIGN KEY(vendor_id) REFERENCES vendor(vendor_id)
);

CREATE TABLE vendor_location (
vendor_id char(10),
address_id char(10), 
vendor_address_country_code varchar(20), 
PRIMARY KEY(address_id), 
FOREIGN KEY(vendor_id) REFERENCES vendor(vendor_id)
);

CREATE TABLE product (
product_id char(10), 
product varchar(100), 
price numeric(5,2), 
vendor_id char(10), 
PRIMARY KEY(product_id), 
FOREIGN KEY(vendor_id) REFERENCES vendor(vendor_id)
);

CREATE TABLE customer (
cust_id char(10),	
customer varchar(30) NOT NULL,	
customer_email varchar(50),	
customer_phone varchar(30),	
PRIMARY KEY(cust_id)
);

CREATE TABLE tip (
tip_id char(10),	
tip numeric(3,2),	
PRIMARY KEY(tip_id)
);

CREATE TABLE promo_code (
promo_code_id char(10),	
promo_code_status varchar(5),	
PRIMARY KEY(promo_code_id)
);

CREATE TABLE rating (
rating_id char(10),	
rating varchar(5),	
PRIMARY KEY(rating_id)
);

CREATE TABLE market_group (
market_group_id char(10),
market_group varchar(100),	
PRIMARY KEY(market_group_id)
);

CREATE TABLE market ( 
market_id char(10),	
market varchar(100),	
market_group_id char(10),	
PRIMARY KEY(market_id),	
FOREIGN KEY(market_group_id) REFERENCES market_group(market_group_id)
);

CREATE TABLE orders (
order_id char(10),	
order_status varchar(20),	
order_updated timestamp,	
market_id char(10),	
cust_id char(10),	
PRIMARY KEY(order_id),	
FOREIGN KEY(market_id) REFERENCES market(market_id),	
FOREIGN KEY(cust_id) REFERENCES customer(cust_id)
);

CREATE TABLE detail_order (
order_id char(10),	
vendor_id char(10),	
product_id char(10),	
cust_id char(10),	
promo_code_id char(10),	
tip_id char(10),
rating_id char(10),
PRIMARY KEY(order_id),	
FOREIGN KEY(vendor_id) REFERENCES vendor(vendor_id),	
FOREIGN KEY(product_id) REFERENCES product(product_id),	
FOREIGN KEY(cust_id) REFERENCES customer(cust_id),	
FOREIGN KEY(promo_code_id) REFERENCES promo_code(promo_code_id),	
FOREIGN KEY(tip_id) REFERENCES tip(tip_id),
FOREIGN KEY(rating_id) REFERENCES rating(rating_id)
);

CREATE TABLE order_note (
note_id char(10),	
order_id char(10),
note text,	
PRIMARY KEY(note_id),	
FOREIGN KEY(order_id) REFERENCES orders (order_id)
);

CREATE TABLE date (
date_id char(10),
order_id char(10),	
order_create_date timestamp,	
order_update_date timestamp,	
removed char(6),	
PRIMARY KEY(date_id),	
FOREIGN KEY(order_id) REFERENCES orders (order_id)
);

CREATE TABLE order_price(
order_price_id char(10),
order_id char(10),	
amount integer,	
subtotal numeric(8,2),	
PRIMARY KEY(order_price_id),	
FOREIGN KEY(order_id) REFERENCES orders (order_id)
);


"

dbGetQuery(con, sql)


install.packages('dplyr')
library(dplyr)

setwd("/Users/antingc/Documents/")
df<- read.csv("Group3_Final_Project2.csv")

head(df)
#head(df2)

#sprintf:takes a vector and format it in a way you define, 
df<-bind_cols('tip_id'=sprintf('t%09d',1:nrow(df)),df)
df<-bind_cols('promo_code_id'=sprintf('pr%08d',1:nrow(df)),df)
df<-bind_cols('rating_id'=sprintf('r%09d',1:nrow(df)),df)
df<-bind_cols('note_id'=sprintf('n%09d',1:nrow(df)),df)
df<-bind_cols('date_id'=sprintf('d%06d',1:nrow(df)),df)
df<-bind_cols('order_price_id'=sprintf('op%05d',1:nrow(df)),df)
df<-bind_cols('delivery_id'=sprintf('de%08d',1:nrow(df)),df)

print(df$order_id_new)
print(df$date_id)
print(df$order_price_id)

#check for missing values
colnames(df)[colSums(is.na(df)) > 0]
rownames(df)[rowSums(is.na(df)) > 0]

#vendor
df1 <- df%>% select(vendor_id, vendor, vendor_create_date) %>% distinct() 
df2 <- df1
dbWriteTable(con,name='vendor',value=df2,
             row.names=FALSE,append=TRUE)

#vendor_contact
df1<-df %>% select(vendor_id, vendor_phone, vendor_website) %>% distinct() 
df2<-bind_cols('vendor_contact_id'=sprintf('vc%08d',1:nrow(df1)),df1)
dbWriteTable(con,name='vendor_contact',value=df2,
             row.names=FALSE,append=TRUE)
df <- left_join(df, df2, by = c('vendor_id','vendor_phone','vendor_website'))

#vendor_location
df1<-df %>% select(vendor_id, address_id, vendor_address_country_code) %>% distinct() 
df2 <- df1
dbWriteTable(con,name='vendor_location',value=df2,
             row.names=FALSE,append=TRUE)

#product
df1<-df %>% select(product, price, vendor_id) %>% distinct()   
df2<-bind_cols('product_id'=sprintf('p%09d',1:nrow(df1)),df1)
dbWriteTable(con,name='product',value=df2,
             row.names=FALSE,append=TRUE)
df <- left_join(df, df2, by = c('product','price','vendor_id'))

#customers
df1<-df %>% select(cust_id, customer, customer_email, customer_phone) %>% distinct()   
df2 <- df1
dbWriteTable(con,name='customer',value=df2,
             row.names=FALSE,append=TRUE)

#tip
df1<-df %>% select(tip, tip_id)  %>% distinct() 
df2 <- df1
dbWriteTable(con,name='tip',value=df2,
             row.names=FALSE,append=TRUE)

#promo_code
df1<-df %>% select(promo_code_status, promo_code_id) %>% distinct() 
df2 <- df1
dbWriteTable(con,name='promo_code',value=df2,
             row.names=FALSE,append=TRUE)

#rating
df1<-df %>% select(rating, rating_id) %>% distinct()   
df2 <- df1
dbWriteTable(con,name='rating',value=df2,
             row.names=FALSE,append=TRUE)
#market_group
df1 <- df%>% select(market_group) %>% distinct() 
df2<-bind_cols('market_group_id'=sprintf('ma%08d',1:nrow(df1)),df1)
dbWriteTable(con,name='market_group',value=df2,
             row.names=FALSE,append=TRUE)
df <- left_join(df, df2, by = c('market_group'))

#market
df1 <- df%>% select(market_id, market, market_group_id) %>% distinct() 
df2 <- df1
dbWriteTable(con,name='market',value=df2,
             row.names=FALSE,append=TRUE)

#orders
df1 <- df%>% select('order_id'=order_id_new, order_status, order_updated, market_id, cust_id) %>% distinct() 
df2 <- df1
dbWriteTable(con,name='orders',value=df2,
             row.names=FALSE,append=TRUE)
print(df)

#detail_order
df1<-df %>% select('order_id'=order_id_new, cust_id, product_id, vendor_id, promo_code_id, tip_id, rating_id)  %>% distinct() 
df2 <- df1
dbWriteTable(con,name='detail_order',value=df2,
             row.names=FALSE,append=TRUE)

#note
df1<-df %>% select('order_id'=order_id_new, note, note_id) %>% distinct()
df2 <- df1
dbWriteTable(con,name='order_note',value=df2,
             row.names=FALSE,append=TRUE)

#date
df1<-df %>% select('order_id'=order_id_new, order_create_date, order_update_date, removed, date_id) %>% distinct()   
df2 <- df1
dbWriteTable(con,name='date',value=df2,
             row.names=FALSE,append=TRUE)

print(df$date_id)
print(df$order_id_new)
nrow(df$order_id_new)
nrow(df)

#order_price
df1<-df %>% select('order_id'=order_id_new, amount, subtotal, order_price_id) %>% distinct()   
df2 <- df1
dbWriteTable(con,name='order_price',value=df2,
             row.names=FALSE,append=TRUE)




;
