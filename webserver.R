########################
#file.names-grep & gsub
########################
txt = readLines("http://eeyore.ucdavis.edu/stat242/data")
ll = grep("csv.bz2", txt)   #line.no. with "csv.bz2"
ll
txt[ll]

#gsub(".*([0-9]{4}).(csv).bz2.*", "\\1\\2", txt[ll])  #"()" represent component, \1 indicates first component
gsub(".*([0-9]{4}.(csv).bz2).*", "\\1", txt[ll])

############################
#file.names-getNodeSet
############################
library(XML)
doc = htmlParse("http://eeyore.ucdavis.edu/stat242/data")
getNodeSet(doc, "//a/@href[contains(., 'csv.bz2')]")
unlist(getNodeSet(doc, "//a/@href[contains(., 'csv.bz2')]"))

#url with the file.names
sprintf("http://eeyore.ucdavis.edu/stat242/data/%s", unlist(getNodeSet(doc, "//a/@href[contains(., 'csv.bz2')]")))
sprintf("http://eeyore.ucdavis.edu/stat242/data/%s", gsub(".*([0-9]{4}.(csv).bz2).*", "\\1", txt[ll]))

#################################
#streaming data from web servers
#################################
ff = sprintf("http://eeyore.ucdavis.edu/stat242/data/%s", unlist(getNodeSet(doc, "//a/@href[contains(., 'csv.bz2')]")))
system("curl -s http://eeyore.ucdavis.edu/stat242/data/1999.csv.bz2 -o - | bunzip2 | grep LAX | wc -l")
#system("curl -s http://eeyore.ucdavis.edu/stat242/data/1999.csv.bz2 -o - | bunzip2 | grep LAX | wc -l")
#383593



