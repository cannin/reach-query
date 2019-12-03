library(GetoptLong)
library(httr)
library(magrittr)
library(readr)
library(simpleRCache)
library(jsonlite)

# Setups
Sys.setenv("PREFIX_SIMPLERCACHE"="reach")
Sys.setenv("DEBUG_SIMPLERCACHE"="TRUE")

cacheDir <- 'cache'
setCacheRootPath(cacheDir)
GETCached <- addMemoization(GET)

url <- "http://reach-api.nrnb-docker.ucsd.edu/"
threshold <- 15

all_queries <- read_csv("queries.csv", col_types = cols(
  Term = col_character(),
  Hits = col_double()
))
queries <- all_queries[which(all_queries$Hits >= threshold), ]
queries <- queries[order(-queries$Hits),]
nrow(queries)

query_entity_text_tmpl <- '
{
  allDocuments(entity_text: "@{entity_text}") {
    pmc_id
    evidence
    extracted_information {
      participant_b {
        identifier
        entity_type
        entity_text
      }
      participant_a {
        identifier
        entity_type
        entity_text
      }
    }
  }
}
'

# NLP_Entity_Hits	NLP_Identifier_Hits
queries$entity_text_pmc_count <- 0
queries$identifier_pmc_count <- 0

query_identifier_tmpl <- '
{
  allDocuments(identifier: "@{identifier}") {
    pmc_id
    evidence
    extracted_information {
      participant_b {
        identifier
        entity_type
        entity_text
      }
      participant_a {
        identifier
        entity_type
        entity_text
      }
    }
  }
}
'

#for(i in 1:10) {
for(i in 1:nrow(queries)) {
  #i <- 2
  cat("I: ", i, " Q: ", queries$Term[i], "\n")
  entity_text <- queries$Term[i]
  identifier <- queries$Term[i]
  
  entity_text_str <- qq(query_entity_text_tmpl)
  identifier_str <- qq(query_identifier_tmpl)
  
  tryCatch({
    resultsHash <- capture.output(tmp <- GETCached(url=url, query=list(query = entity_text_str), add_headers(Accept = "application/json")))
    resultsHash <- resultsHash %>% trimws %>% sub("keyHash:  ", "", .)
    cat("HASH: D1: ", resultsHash, "\n")
    
    results <- content(tmp, "text")
    
    if(tmp$status_code != 200 || grepl("ECONNREFUSED", results)) {
      badCacheFile <- dir(cacheDir, pattern=resultsHash)
      file.remove(file.path(cacheDir, badCacheFile))
      cat("ERROR: D1: I: ", i, " Q: ", queries$Term[i], "\n")
    } else {
      tmp <- fromJSON(results)
      d1 <- tmp$data$allDocuments
      
      if(length(d1) > 0) {
        queries$entity_text_pmc_count[i] <- unique(d1$pmc_id) %>% length
      } else {
        cat("WARNING: D1: EMPTY: I: ", i, " Q: ", queries$Term[i], "\n")
      }
    }
  }, error = function(err) {
    stop(paste("ERROR TRY: D1: ", err, "\n"))
  })
  
  tryCatch({
    resultsHash <- capture.output(tmp <- GETCached(url=url, query = list(query = identifier_str), add_headers(Accept = "application/json")))
    resultsHash <- resultsHash %>% trimws %>% sub("keyHash:  ", "", .)
    cat("HASH: D2: ", resultsHash, "\n")
    
    results <- content(tmp, "text")
    
    if(tmp$status_code != 200 || grepl("ECONNREFUSED", results)) {
      badCacheFile <- dir(cacheDir, pattern=resultsHash)
      file.remove(file.path(cacheDir, badCacheFile))
      cat("ERROR: D2: I: ", i, " Q: ", queries$Term[i], "\n")
    } else {
      tmp <- fromJSON(results)
      d2 <- tmp$data$allDocuments
      
      if(length(d2) > 0) {
        queries$identifier_pmc_count[i] <- unique(d2$pmc_id) %>% length
      } else {
        cat("WARNING: D2: EMPTY: I: ", i, " Q: ", queries$Term[i], "\n")
      }
    }
  }, error = function(err) {
    stop(paste("ERROR TRY: D2: ", err, "\n"))
  })
  
  write_csv(queries, "queries_out.csv")
}

stop("STOP")

library(future.apply)

plan(multiprocess, workers=2)

r <- future.apply::future_sapply(1:nrow(queries), function(i) {
#r <- sapply(1:nrow(queries), function(i) {
  # i <- 113
  # Setups
  Sys.setenv("PREFIX_SIMPLERCACHE"="reach")
  Sys.setenv("DEBUG_SIMPLERCACHE"="TRUE")
  
  cacheDir <- 'cache'
  setCacheRootPath(cacheDir)
  GETCached <- addMemoization(GET)
  
  url <- "http://reach-api.nrnb-docker.ucsd.edu/"
  threshold <- 15 
  
  all_queries <- suppressWarnings(suppressMessages(read_csv("queries.csv", col_types = cols(
    Term = col_character(),
    Hits = col_double()
  ))))
  queries <- all_queries[which(all_queries$Hits >= threshold), ]
  queries <- queries[order(-queries$Hits),]
  nrow(queries)
  
  query_entity_text_tmpl <- '
{
  allDocuments(entity_text: "@{entity_text}") {
    pmc_id
    evidence
    extracted_information {
      participant_b {
        identifier
        entity_type
        entity_text
      }
      participant_a {
        identifier
        entity_type
        entity_text
      }
    }
  }
}
'
  
  query_identifier_tmpl <- '
{
  allDocuments(identifier: "@{identifier}") {
    pmc_id
    evidence
    extracted_information {
      participant_b {
        identifier
        entity_type
        entity_text
      }
      participant_a {
        identifier
        entity_type
        entity_text
      }
    }
  }
}
'
  
  # NLP_Entity_Hits	NLP_Identifier_Hits
  queries$entity_text_pmc_count <- 0
  queries$identifier_pmc_count <- 0
  
  #i <- 1
  cat("I: ", i, " Q: ", queries$Term[i], "\n")
  entity_text <- queries$Term[i]
  identifier <- queries$Term[i]
  
  entity_text_str <- qq(query_entity_text_tmpl)
  identifier_str <- qq(query_identifier_tmpl)
  
  tryCatch({
    resultsHash <- capture.output(tmp <- GETCached(url=url, query=list(query = entity_text_str), add_headers(Accept = "application/json")))
    resultsHash <- resultsHash %>% trimws %>% sub("keyHash:  ", "", .)
    
    results <- content(tmp, "text")
    
    if(tmp$status_code != 200 || grepl("ECONNREFUSED", results)) {
      badCacheFile <- dir(cacheDir, pattern=resultsHash)
      file.remove(file.path(cacheDir, badCacheFile))
      cat("ERROR: D1: I: ", i, " Q: ", queries$Term[i], "\n")
    } else {
      tmp <- fromJSON(results)
      d1 <- tmp$data$allDocuments
      
      if(length(d1) > 0) {
        queries$entity_text_pmc_count[i] <- unique(d1$pmc_id) %>% length
      } else {
        cat("WARNING: D1: EMPTY: I: ", i, " Q: ", queries$Term[i], "\n")
      }
    }
  }, error = function(err) {
    stop(paste("ERROR TRY: D1: ", err, "\n"))
  })
  
  tryCatch({
    resultsHash <- capture.output(tmp <- GETCached(url=url, query = list(query = identifier_str), add_headers(Accept = "application/json")))
    resultsHash <- resultsHash %>% trimws %>% sub("keyHash:  ", "", .)
    
    results <- content(tmp, "text")
    
    if(tmp$status_code != 200 || grepl("ECONNREFUSED", results)) {
      badCacheFile <- dir(cacheDir, pattern=resultsHash)
      file.remove(file.path(cacheDir, badCacheFile))
      cat("ERROR: D2: I: ", i, " Q: ", queries$Term[i], "\n")
    } else {
      tmp <- fromJSON(results)
      d2 <- tmp$data$allDocuments
      
      if(length(d2) > 0) {
        queries$identifier_pmc_count[i] <- unique(d2$pmc_id) %>% length
      } else {
        cat("WARNING: D2: EMPTY: I: ", i, " Q: ", queries$Term[i], "\n")
      }
    }
  }, error = function(err) {
    stop(paste("ERROR TRY: D2: ", err, "\n"))
  })
})

