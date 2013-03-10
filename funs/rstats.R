#rstats 
rstats<-function(B,con,year){
  B=as.integer(B)
  #set up counters
  Rcounts<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  Rsum<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  Rsumvar<-structure(c(data=rep(0,4)),names=c("LAX","OAK","SFO","SMF"))
  #reading blocks
  while(TRUE){
    txt=readLines(con,n=B)
    if (length(txt)==0)
      break
    #ArrDelay & Airport
    ArrDelay=as.numeric(sapply(strsplit(txt,","),"[[",15))
    Origin=as.factor(sapply(strsplit(txt,","),"[[",17))
    #dropNA
    dropNA<-!is.na(ArrDelay)
    ArrDelay<-ArrDelay[dropNA]
    Origin<-Origin[dropNA]
    #1.1 Rcounts
    update<-as.numeric(table(temp)[names(Rcounts)])
    #update<-tapply(ArrDelay,Origin,length)[names(Rcounts)] #table() is faster than tapply(length)
    update[is.na(update)]<-0
    Rcounts<-Rcounts+as.numeric(update)
    #1.2 Rsum
    update<-tapply(ArrDelay,Origin,sum)[names(Rsum)]
    update[is.na(update)]<-0
    Rsum<-Rsum+as.numeric(update)
    #1.3 Rvar
    update<-tapply(ArrDelay,Origin,function(x){var(x)*length(x)})[names(Rsumvar)]
    update[is.na(update)]<-0
    Rsumvar<-Rsumvar+as.numeric(update) 
  }
  cbind(Rcounts,Rsum,Rsumvar)
}
