library(RODBC)#由于转债的个数较少，暂时不用像股票一样做拼接
library(matlab)
library(readxl)
conn = odbcConnect("v-wind",'ywpublic','1qazXSW@')#连接wind数据库

#today <- Sys.Date()
#year <- substr(today,1,4)
#month <- substr(today,6,7)
#day <- substr(today,9,10)
#end <- paste(year,month,day,sep = "")

start <- '20011231'
end <- '20190920'                               #确定起止时间

idch <- "SELECT S_INFO_WINDCODE,B_INFO_LISTDATE,B_INFO_DELISTDATE
FROM wind_quant.dbo.CBondDescription
WHERE B_INFO_SPECIALBONDTYPE = '可转债'
AND B_INFO_ISSUETYPE != 439030000
AND (S_INFO_EXCHMARKET = 'SSE' OR S_INFO_EXCHMARKET = 'SZSE')
AND B_INFO_LISTDATE >0
ORDER BY B_INFO_LISTDATE"
idco <- sqlQuery(conn,idch)

id <- as.matrix(idco$S_INFO_WINDCODE)
li <- length(id)

datech <- paste("SELECT trade_dt
                FROM wind_quant.dbo.AIndexEODPrices                                 
                WHERE S_INFO_WINDCODE='000001.SH'
                AND trade_dt BETWEEN '",start,"' AND '",end,"'" ,
                sep='') 
dateco <- sqlQuery(conn,datech)
date <- as.matrix(dateco)                       #用上证综指的能提取出的日期作为交易日期
ld <- length(date)

month <- matrix(0,ld,2)
month[,1] <- as.matrix(date)
for (i in 1:ld){
  month[i,2] <- as.numeric(substr(month[i,1],5,6))
}                                               #通过日期列得到每个日期对应的月份

par <- matrix(0,ld,li)                          #par用于记录转债面额
amount <- matrix(0,ld,li)                       #amount用于记录转债日度成交数据（日度更新表中用不到）
price <- matrix(0,ld,li)                        #price用于记录转债日度价格数据 
ini20 <- matrix(0,ld,li)                        #ini20用于记录转债是否上市满20日
conv <- matrix(0,ld,li)                         #conv用于记录转股溢价率（日度更新表中用不到）
YTM <- matrix(0,ld,li)                          #用于记录YTM
ratio <- matrix(0,ld,li)                        #ratio用于记录转股价值/纯债价值
int <- matrix(0,ld,li)                          #int用于记录转债付息数据
price20 <- matrix(0,ld,li)                      #price20是price中的数据消除了上市前20的数据

for (i in 1:li){                                                #提取发行日期，记入ini
  inich <- paste("SELECT cb_issue_anncelstdate
                 FROM wind_quant.dbo.CCBondIssuance
                 WHERE S_INFO_WINDCODE='",id[c(i)],
                 "'",sep='')
  inico <- sqlQuery(conn,inich)
  if (inico<end && !is.na(inico)){
    temp <- date-inico$cb_issue_anncelstdate
    temp[temp<0]=100000
    tag <- find(temp == min(temp)) 
    
    if(length(tag)>0&&(tag+20)<=ld){
      ini20[(tag+20):ld,i] <- 1
    }
  }
  print(i)
}

ini20[1:20,] <- 1

for (i in 1:li){                                               #提出剩余面额，记入par
  sharech <- paste("SELECT s_info_enddate,b_info_outstandingbalance
                   FROM wind_quant.dbo.CBondAmount
                   WHERE S_INFO_WINDCODE='",id[c(i)],"'
                   AND s_info_enddate BETWEEN '",start,"' AND '",end,"'" ,
                   sep='') 
  shareco <- sqlQuery(conn,sharech)
  share <- as.matrix(shareco[order(shareco[,1]),])
  ls <- length(share[,1])
  if (ls>0){
    for (j in 1:ls){
      temp <- date-share[j,1]
      temp[temp<0] <- 100000
      tag <- find(temp == min(temp))
      par[tag:ld,i] <- share[j,2]
    }
  }
}

for (i in 1:li){                                               #提出成交量和价格，记入amount和price
  amountch <- paste("SELECT b_dq_amount,trade_dt,B_DQ_ORIGINCLOSE
                    FROM wind_quant.dbo.CBondPrices
                    WHERE s_info_windcode = '",id[c(i)],
                    "' AND trade_dt BETWEEN '",start,"' AND '",end,"'" ,             
                    sep='')
  amountco <- sqlQuery(conn,amountch)
  
  la <-length(amountco[,1])
  if(la>0){
    for (k in 1:la){
      tag <- find(date==amountco[k,2])
      amount[tag,i] <- amountco[k,1]
      price[tag,i] <- amountco[k,3]
    }
  }
  print(i)
}

for (i in 1:li){                                               #提出转债相关指标，包括转股溢价率、转股价值、纯债价值和YTM
  convch <- paste("SELECT trade_dt,cb_anal_convpremiumratio,CB_ANAL_STRBVALUE,CB_ANAL_CONVVALUE,CB_ANAL_YTM
                  FROM wind_quant.dbo.CCBondValuation
                  WHERE s_info_windcode = '",id[c(i)],
                  "' AND trade_dt BETWEEN '",start,"' AND '",end,"'" ,            
                  sep='')
  convco <- sqlQuery(conn,convch)
  
  
  
  
  
  lc <- length(convco[,1])
  if(lc>0){
    for (p in 1:lc){
      if (!is.na(convco[p,2])){
        tag <- find(date==convco[p,1])
        conv[tag,i] <- convco[p,2]
      }
      if (!is.na(convco[p,3])&&!is.na(convco[p,4])){
        tag <- find(date==convco[p,1])
        ratio[tag,i] <- convco[p,4]/convco[p,3]
      }
      if (!is.na(convco[p,5])){
        tag <- find(date==convco[p,1])
        YTM[tag,i] <- convco[p,5]
      }
    }
    
  }
  
  print(i)
}

for (i in 1:li){
  price20[,i] <- ini20[,i]*price[,i]
}

for (i in 1:li){                                               #提出付息信息
  intch <- paste("SELECT b_info_paymentdate,b_info_interestperthousands
                 FROM wind_quant.dbo.CBondPayment
                 WHERE s_info_windcode = '",id[c(i)],
                 "' AND b_info_paymentdate BETWEEN'",start,"' AND '",end,"'" ,             
                 sep='')
  intco <- sqlQuery(conn,intch)
  ll=length(intco$b_info_paymentdate)
  if(ll>0){
    for (j in 1:ll){
      test <- date-intco$b_info_paymentdate[c(j)]
      test[test<0] <- 100000
      tag <- find(test==min(test))
      int[tag,i]=intco$b_info_interestperthousands[c(j)]
    }
  }
}

#数据读入部分完成
#
#以下是指数编制部分

sig1 <- par-0.3                   #面额是否满0.3亿的信号
sig1[sig1<=0] <- 0
sig1[sig1>0] <- 1

cash <- (0.03/365)

sig2 <- price20                #20日入库是否有价格的信号                  
sig2[sig2>0] <- 1

sig4 <- ratio                     #是否平衡信号
sig4[sig4>1.2] <- 0
sig4[sig4<0.8] <- 0
sig4[sig4>0] <- 1

index <- matrix(0,ld,25)

index[1,3] <- 100
index[1,4] <- 100
index[1,5] <- 100               #设定指数的初值，3、4、5分别存储等权指数、加权指数和限制指数

for (i in 2:ld){    #等权
  if (month[i,2]!=month[i-1,2]){              #当日月数据与昨日不同触发换月换券的条件
    temp <- sig1[i,]*sig2[i,]*sig2[i-1,]*sig4[i,]*sig4[i-1,]             #temp是在一整个月当中共用的信号，包含了是否满0.3亿、是否上市满20日、是否平衡型
  }
  temp1 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*sig1[i,]*sig2[i-1,]*temp)))           #temp1为等权模拟组合的今日净值，再乘一遍价格和面额信号是为了防止转债到期或赎回
  temp2 <- sum(rowSums(as.matrix(price20[i-1,]*sig1[i,]*sig2[i,]*temp)))                        #temp2位等权模拟组合的昨日净值
  temp3 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*par[i,]*sig1[i,]*sig2[i-1,]*temp)))   #temp3为加权模拟组合的今日净值
  temp4 <- sum(rowSums(as.matrix(price20[i-1,]*par[i,]*sig1[i,]*sig2[i,]*temp)))                #temp4为加权模拟组合的昨日净值
  
  index[i,1] <- sum(rowSums(as.matrix(par[i,]*sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#规模
  index[i,2] <- sum(rowSums(as.matrix(sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#个数
  if (temp2!=0){             #保证昨日净值不为零
    index[i,3] <- temp1*index[i-1,3]/temp2        #指数单日增长率=模拟组合单日增长率
    if (index[i,2]>=10){                          #转债个数超过10个时限制指数=等权指数
      index[i,4] <- temp1*index[i-1,4]/temp2
    }else{                                        #小于10个时放回购，回购利率简单假定为3%
      index[i,4] <- (temp1*index[i,2]/(temp2*10)+(0.03/365+1)*(1-index[i,2]/10))*index[i-1,4]
    }
  }else{                     #若昨日净值为0（也即没有转债）则无限制指数不变，限制指数放回购
    index[i,3] <- index[i-1,3]
    index[i,4] <- index[i-1,4]*(0.03/365+1)
  }
  if (temp4!=0){             #等权部分净值同上处理
    index[i,5] <- temp3*index[i-1,5]/temp4
  }else{
    index[i,5] <- index[i-1,5]
  }
}

sig4 <- ratio                     #是否偏股信号
sig4[sig4<1.2] <- 0
sig4[sig4>0] <- 1

index[1,8] <- 100
index[1,9] <- 100
index[1,10] <- 100

for (i in 2:ld){    #等权
  if (month[i,2]!=month[i-1,2]){
    temp <- sig1[i,]*sig2[i,]*sig2[i-1,]*sig4[i,]*sig4[i-1,]
  }
  temp1 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*sig1[i,]*sig2[i-1,]*temp)))
  temp2 <- sum(rowSums(as.matrix(price20[i-1,]*sig1[i,]*sig2[i,]*temp)))
  temp3 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*par[i,]*sig1[i,]*sig2[i-1,]*temp)))
  temp4 <- sum(rowSums(as.matrix(price20[i-1,]*par[i,]*sig1[i,]*sig2[i,]*temp)))
  
  index[i,6] <- sum(rowSums(as.matrix(par[i,]*sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#规模
  index[i,7] <- sum(rowSums(as.matrix(sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#个数
  if (temp2!=0){
    index[i,8] <- temp1*index[i-1,8]/temp2
    if (index[i,7]>=10){
      index[i,9] <- temp1*index[i-1,9]/temp2
    }else{
      index[i,9] <- (temp1*index[i,7]/(temp2*10)+(0.03/365+1)*(1-index[i,7]/10))*index[i-1,9]
    }
  }else{
    index[i,8] <- index[i-1,8]
    index[i,9] <- index[i-1,9]*(0.03/365+1)
  }
  if (temp4!=0){
    index[i,10] <- temp3*index[i-1,10]/temp4
  }else{
    index[i,10] <- index[i-1,10]
  }
}


sig4 <- ratio                     #是否偏债信号
sig4[sig4>0.8] <- 0
sig4[sig4>0] <- 1

index[1,13] <- 100
index[1,14] <- 100
index[1,15] <- 100

for (i in 2:ld){    #等权
  if (month[i,2]!=month[i-1,2]){
    temp <- sig1[i,]*sig2[i,]*sig2[i-1,]*sig4[i,]*sig4[i-1,]
  }
  temp1 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*sig1[i,]*sig2[i-1,]*temp)))
  temp2 <- sum(rowSums(as.matrix(price20[i-1,]*sig1[i,]*sig2[i,]*temp)))
  temp3 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*par[i,]*sig1[i,]*sig2[i-1,]*temp)))
  temp4 <- sum(rowSums(as.matrix(price20[i-1,]*par[i,]*sig1[i,]*sig2[i,]*temp)))
  
  index[i,11] <- sum(rowSums(as.matrix(par[i,]*sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#规模
  index[i,12] <- sum(rowSums(as.matrix(sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#个数
  if (temp2!=0){
    index[i,13] <- temp1*index[i-1,13]/temp2
    if (index[i,12]>=10){
      index[i,14] <- temp1*index[i-1,14]/temp2
    }else{
      index[i,14] <- (temp1*index[i,12]/(temp2*10)+(0.03/365+1)*(1-index[i,12]/10))*index[i-1,14]
    }
  }else{
    index[i,13] <- index[i-1,13]
    index[i,14] <- index[i-1,14]*(0.03/365+1)
  }
  if (temp4!=0){
    index[i,15] <- temp3*index[i-1,15]/temp4
  }else{
    index[i,15] <- index[i-1,15]
  }
}

sig4 <- YTM                       #是否YTM>2
sig4[sig4<2] <- 0
sig4[sig4>0] <- 1

index[1,18] <- 100
index[1,19] <- 100
index[1,20] <- 100

for (i in 2:ld){    #等权
  if (month[i,2]!=month[i-1,2]){
    temp <- sig1[i,]*sig2[i,]*sig2[i-1,]*sig4[i,]*sig4[i-1,]
  }
  temp1 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*sig1[i,]*sig2[i-1,]*temp)))
  temp2 <- sum(rowSums(as.matrix(price20[i-1,]*sig1[i,]*sig2[i,]*temp)))
  temp3 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*par[i,]*sig1[i,]*sig2[i-1,]*temp)))
  temp4 <- sum(rowSums(as.matrix(price20[i-1,]*par[i,]*sig1[i,]*sig2[i,]*temp)))
  
  index[i,16] <- sum(rowSums(as.matrix(par[i,]*sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#规模
  index[i,17] <- sum(rowSums(as.matrix(sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#个数
  if (temp2!=0){
    index[i,18] <- temp1*index[i-1,18]/temp2
    if (index[i,17]>=10){
      index[i,19] <- temp1*index[i-1,19]/temp2
    }else{
      index[i,19] <- (temp1*index[i,17]/(temp2*10)+(0.03/365+1)*(1-index[i,17]/10))*index[i-1,19]
    }
  }else{
    index[i,18] <- index[i-1,18]
    index[i,19] <- index[i-1,19]*(0.03/365+1)
  }
  if (temp4!=0){
    index[i,20] <- temp3*index[i-1,20]/temp4
  }else{
    index[i,20] <- index[i-1,20]
  }
}

sig4 <- matrix(1,ld,li)           #总指数

index[1,23] <- 100
index[1,24] <- 100
index[1,25] <- 100

for (i in 2:ld){    #等权
  if (month[i,2]!=month[i-1,2]){
    temp <- sig1[i,]*sig2[i,]*sig2[i-1,]*sig4[i,]*sig4[i-1,]
  }
  temp1 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*sig1[i,]*sig2[i-1,]*temp)))
  temp2 <- sum(rowSums(as.matrix(price20[i-1,]*sig1[i,]*sig2[i,]*temp)))
  temp3 <- sum(rowSums(as.matrix((price20[i,]+int[i,]/10)*par[i,]*sig1[i,]*sig2[i-1,]*temp)))
  temp4 <- sum(rowSums(as.matrix(price20[i-1,]*par[i,]*sig1[i,]*sig2[i,]*temp)))
  
  index[i,21] <- sum(rowSums(as.matrix(par[i,]*sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#规模
  index[i,22] <- sum(rowSums(as.matrix(sig1[i,]*sig2[i-1,]*sig2[i,]*temp)))#个数
  if (temp2!=0){
    index[i,23] <- temp1*index[i-1,23]/temp2
    if (index[i,22]>=10){
      index[i,24] <- temp1*index[i-1,24]/temp2
    }else{
      index[i,24] <- (temp1*index[i,22]/(temp2*10)+(0.03/365+1)*(1-index[i,22]/10))*index[i-1,24]
    }
  }else{
    index[i,23] <- index[i-1,23]
    index[i,24] <- index[i-1,24]*(0.03/365+1)
  }
  if (temp4!=0){
    index[i,25] <- temp3*index[i-1,25]/temp4
  }else{
    index[i,25] <- index[i-1,25]
  }
}

write.table(index,"cbond/update/index.csv",col.names=F,row.names=F,sep=",")