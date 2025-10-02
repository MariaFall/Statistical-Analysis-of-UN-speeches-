library(tidyverse)
library(readtext)
library(quanteda)
library(fuzzyjoin)
library(glue)
library(openxlsx)
library(vroom)
library(tidytext)
library(furrr) 
library(future)
library(progressr) 
library(igraph)
library(gridExtra)
library(GGally)
library(ggpubr)
library(grid)

#parallel processing
plan(multisession, workers = 12) #depends on how many cores are available
handlers(global = TRUE)
handlers("txtprogressbar")

#=========##=========##=========##=========##=========##=========##=========#
#=========##=========##=========#Functions Declarations#=========##=========#
#=========##=========##=========##=========##=========##=========##=========#

ToksToEngrams = function (TokensToEngram, index) {
  TokensToEngram1 = as.tokens_xptr(TokensToEngram)
  NgramBuffer = tokens_ngrams(TokensToEngram1, n = index, concatenator = " ")
  return(NgramBuffer)
}

FindKeywords = function (DF, TokensObject) {
  KWHitList = list()
  DF$token_count <- sapply(strsplit(DF$rx, "\\s+"), length)
  maxTokenCount = as.integer(max(DF$token_count))
  
  KWHitList[[length(KWHitList)+1]] <- future_map(1:maxTokenCount, function(i) {
    NgramBuff = ToksToEngrams(TokensToEngram = TokensObject, index = i)
    DFBuff = DF %>% filter(token_count == i)
    resultBuff = kwic(
      NgramBuff,
      pattern = DFBuff$rx,
      valuetype = "regex",
      window = 0
    )
    #print(str_c("Index is finished: ", as.character(i)))
    as.data.frame(resultBuff)
  }, .progress = TRUE, .options = furrr_options(seed = 1))
  
  FoundKeywordsDF = bind_rows(KWHitList)
  
  lilBuff = DF %>% select(rx, token_count)
  names(lilBuff) = c("pattern", "token_count")
  
  FoundKeywordsDF = left_join(FoundKeywordsDF, lilBuff, by = "pattern", relationship = "many-to-many")
  
  return(FoundKeywordsDF)
}


FilterDF = function (df) {
  
  FilteredDF_list = list()
  
  df$id= rownames(df)
  df = as.data.frame(df)
  
  #intervals and add a unique row_id for joining
  df_intervals <- df %>%
    mutate(
      start = from,
      end = from + token_count - 1,
      row_id = row_number() #unique ID for each row
    )
  
  FilteredDF_list[[length(FilteredDF_list)+1]] <- df_intervals %>%
    group_by(docname) %>%
    group_split() %>% #split the data frame into a list of data frames, one per docname
    future_map(function(doc_df) { 
      #all pairs of rows with overlapping intervals within each document
      overlapping_pairs <- doc_df %>%
        inner_join(doc_df, by = "docname", suffix = c("_1", "_2"), relationship = "many-to-many") %>%
        filter(
          row_id_1 < row_id_2, #prevents comparing a row to itself and getting duplicate pairs
          start_1 <= end_2,    #the condition for two intervals to overlap
          end_1 >= start_2
        ) %>%
        select(row_id_1, row_id_2) # Select pairs for graph creation
      
      #graph from the pairs and find the connected components (the groups)
      graph <- graph_from_data_frame(overlapping_pairs, directed = FALSE)
      
      #not all rows have overlaps, adding them to the graph as single-node groups
      all_row_ids_in_graph <- as.integer(V(graph)$name)
      all_row_ids_in_df <- doc_df$row_id
      isolated_row_ids <- setdiff(all_row_ids_in_df, all_row_ids_in_graph)
      graph <- add_vertices(graph, nv = length(isolated_row_ids), attr = list(name = isolated_row_ids))
      
      #assign id to each row based on which component it belongs to
      components <- components(graph)
      group_membership <- tibble(
        row_id = as.integer(names(components$membership)),
        group_id = components$membership
      )
      
      #join by group id nad select highest token count
      doc_df %>%
        left_join(group_membership, by = "row_id") %>%
        group_by(docname, group_id) %>%
        slice_max(n = 1, order_by = token_count, with_ties = FALSE) %>%
        ungroup() %>%
        select(id, docname, from, to, pattern, keyword, token_count)
    }, .progress = TRUE, .options = furrr_options(seed = 1)) 
  
  FileteredDF <- bind_rows(FilteredDF_list)
  
  print("it got past bind_rows in FilterDF")
  
  return(FileteredDF)
}

PrepareDataFrame = function (inputDF, toks) {
  #runs consecutive functions to simplify code
  TokTest = tokens_tolower(toks) 
  TokTest1 = tokens(TokTest, remove_punct = T)
  
  BoundDF = FindKeywords(DF = inputDF, TokensObject = TokTest1)
  FilteredDataFrame = FilterDF(df = BoundDF)
  
  return(FilteredDataFrame)
}

PrepareMBA_kw = function (inputAMBDF, toks) {
  #tokenization applied to the full toks`object.
  TokTest = tokens_tolower(toks)
  TokTest1 = tokens(TokTest, remove_punct = T)
  TokTest2 = as.tokens_xptr(TokTest1)
  
  state_adj <- c("international", "national", "state", "governmental", "sovereign", "federal", "central")
  rx_state <- paste0("(?i)\\b(", paste(state_adj, collapse = "|"), ")\\b") #just a regex
  
  kw_amb <- kwic(TokTest2, pattern = inputAMBDF$rx, valuetype = "regex", window = 4) %>% #inputAMBDF$rx for pattern
    as_tibble() %>%
    rowwise() %>%
    mutate(ctx = paste(pre, post, collapse = " "),
           state_hit = str_detect(ctx, rx_state)) %>%
    ungroup() %>%
    filter(!state_hit) %>%
    select(docname, from, to, keyword) %>% 
    group_by(docname, from) %>%
    slice_head(n = 1) %>%
    ungroup()
  
  kw_amb$token_count = sapply(strsplit(kw_amb$keyword, "\\s+"), length)
  
  return(kw_amb)
}


CreatePlots = function (PlotType, TheInputDF) {
  
  PlotList = list()
  
  if (PlotType == "timeseries") {
    
    for (l in 1:8) {
      TheInputDF$mean_rate_per1kBuff = unlist(as.list(TheInputDF[, 17+l]))
      TheInputDF$mean_rate_per1kBuff = as.numeric(TheInputDF$mean_rate_per1kBuff)
      
      if (l <8) {
        plotbuff = ggplot(TheInputDF, aes(x = year, y = mean_rate_per1kBuff, color = country_org)) +
          geom_line(linewidth = 1.5) +
          geom_point(size =2, alpha = 0.8) +
          labs(title = str_c(paste(names(TheInputDF)[2+l]), "- Mentions per 1,000 Tokens "),
               x = "Year",
               y = "Mentions per 1,000 Tokens",
               color = "Country") +
          theme_minimal(base_size = 13) +
          theme(
            legend.title = element_text(size = 12, face = "bold"),
            legend.text = element_text(size = 11),
            legend.position = "right", 
            legend.key.height = unit(1.2, "lines")
          )
        
        PlotList[[l]] = plotbuff
        #print(plotbuff)
      }
      
      if (l ==8) {
        TheInputDF$mean_rate_per1kBuff == unlist(as.list(TheInputDF[, 31]))
        TheInputDF$mean_rate_per1kBuff = as.numeric(TheInputDF$mean_rate_per1kBuff)
        
        plotbuff = ggplot(TheInputDF, aes(x = year, y = mean_rate_per1kBuff, color = country_org)) +
          geom_line(linewidth = 1.5) +
          geom_point(size =2, alpha = 0.8) +
          labs(title = "All Human Security Categories - Mentions per 1,000 Tokens",
               x = "Year",
               y = "Mentions per 1,000 Tokens",
               color = "Country") +
          theme_minimal(base_size = 13) +
          theme(
            legend.title = element_text(size = 12, face = "bold"),
            legend.text = element_text(size = 11),
            legend.position = "right", 
            legend.key.height = unit(1.2, "lines")
          )
        
        PlotList[[l]] = plotbuff
        #print("running create plot grid")
        CreatePlotGrid(PlotList = PlotList, GridTitle = "HS_category_mentions_by_year")
      }
    }
  }
  
  if(PlotType == "Top Agenda") {
    
    TopAgendasBar = ggplot(TheInputDF, aes(x = n, y = reorder(agenda_item_manual, n), fill = hs_cat)) +
      geom_col(show.legend = T) +
      labs(title = "Top Agendas by HS Category Mentions",
           x = "Mentions", y = "Agenda", fill = "HS Category") +
      theme_minimal(base_size =7)
    
    ggsave(plot = TopAgendasBar, filename = "TopAgendas.pdf", device = "pdf")
  }
}


CreatePlotGrid = function (PlotList, GridTitle) {
  #saves plot grid as pdf
  
  pdf(paste(str_c(GridTitle, ".pdf")), width = 22, height = 15)
  grid.arrange(grobs = PlotList, ncol =3, nrow =3)
  dev.off()
  
}

#=========##=========##=========##=========##=========##=========##=========#
#=========##=========##=========#Loading Required Data#=========##=========#
#=========##=========##=========##=========##=========##=========##=========#

setwd(here::here())

hs <- read_delim("hs_dictionary.csv", delim = ";",
                 col_names = c("pattern", "category"),
                 col_types = cols(.default = "c"), show_col_types = FALSE) %>%
  filter(!pattern %in% c("pattern", "") &
           category %in% c("Community Security", "Economic Security",
                           "Environmental Security", "Food Security",
                           "Health Security", "Personal Security",
                           "Political Security")) %>%
  mutate(pattern_length = nchar(pattern)) %>%
  arrange(desc(pattern_length)) %>%
  mutate(rx = paste0("\\b(?:", pattern, ")\\b"))

nsa <- read_delim("nonstate_dictionary.csv", delim = ";", show_col_types = FALSE) %>%
  mutate(rx = paste0("\\b(?:", pattern, ")\\b"))

amb <- read_delim("nonstate_dictionary_ambiguous.csv", delim = ";", show_col_types = FALSE) %>%
  mutate(rx = paste0("\\b(?:", pattern, ")\\b"))

rx_nsa <- paste0("\\b(", paste(nsa$pattern, collapse = "|"), ")\\b")
load("docs_meta.RData")

meta_speeches <- get("meta_speeches") %>%
  mutate(filename = tools::file_path_sans_ext(filename))

texts_df <- readtext("speeches/*.txt", encoding = "UTF-8") %>%
  mutate(filename = tools::file_path_sans_ext(basename(doc_id)))

speeches_full <- inner_join(texts_df, meta_speeches, by = "filename") %>% #cuts ~ 70k speeches - makes sense cuz selecting for countries
  mutate(
    country_org = str_squish(country_org),
    country_org = case_when(
      str_detect(country_org, regex("united\\s+states", TRUE))  ~ "USA",
      str_detect(country_org, regex("united\\s+kingdom", TRUE)) ~ "UK",
      country_org == "Russian Federation"                       ~ "Russia",
      TRUE ~ country_org
    ),
    year = if_else(is.na(year), as.integer(str_sub(filename, 6, 9)), year)
  ) %>%
  filter(country_org %in% c("USA", "UK", "France", "Russia", "China",
                            "Japan", "Canada", "Sweden", "Norway",
                            "India", "Brazil", "Egypt", "Peru", "Thailand"))

corp <- corpus(speeches_full, text_field = "text", docid_field = "filename")

docvars(corp, c("year", "country_org", "spv")) <- speeches_full[, c("year", "country_org", "spv")]

meta_small <- speeches_full %>%
  select(docname = filename, spv, country_org, year, everything())

dash_chars <- c("\u2010", "\u2011", "\u2012", "\u2013", "\u2014") #unicode dash chars
toks <- tokens(corp, remove_punct = FALSE) %>% #class tokens
  tokens_replace(pattern = dash_chars, replacement = rep(" ", length(dash_chars)))

#=========##=========##=========##=========##=========##=========##=========#
#=========##=========##=========#Running Functions / Data processing#=========#
#=========##=========##=========##=========##=========##=========##=========#

HS_kw = PrepareDataFrame(inputDF = hs, toks = toks)
NSA_kw = PrepareDataFrame(inputDF = nsa, toks = toks)
AMB_kw = PrepareMBA_kw(inputAMBDF = amb, toks = toks)

NSA_kwBuff = NSA_kw %>% 
  select(docname, from, to, keyword, token_count) 

kw_actor = bind_rows(NSA_kwBuff, AMB_kw)

names(kw_actor)[4] = "nsa_term" #renaming keyword to nsa_term

MergeBuff2 = hs %>% select(rx, category)
names(MergeBuff2) = c("pattern", "hs_cat")

HS_kwBuff = left_join(HS_kw, MergeBuff2, by = "pattern", relationship = "many-to-many")
HS_kwBuff = HS_kwBuff %>% distinct(id, .keep_all = T) 

HS_kwBuff1 = HS_kwBuff %>% 
  mutate(from = as.integer(from), to = as.integer(to)) %>% 
  select(docname, from, to, hs_term = keyword, hs_cat, token_count) 

join_hits1 <- HS_kwBuff1 %>%
  inner_join(kw_actor, by = "docname", relationship = "many-to-many") %>%
  filter(abs(from.x - from.y) <= (token_count.y + token_count.x +8)) %>% #do 8 since sum tok min is 2 and max is 15 = mean is 8.5 
  rename(HS_idx = from.x, actor_idx = from.y, actor = nsa_term)          #20 is too much, from where HS starts to actor starts + HS/NSA token length + 8

Hits <- join_hits1 %>%
  filter(hs_term != actor)%>%  #since some nsa = hs, filter those out first
  distinct(HS_idx, actor, .keep_all = T) #adjust for overlap of terms


#=======##=======##=======##=======##=======##=======##=======##=======#
#=======##=======##=======##=======#Final Data Mixing#=======##=======#
#=======##=======##=======##=======##=======##=======##=======##=======#


hs_df1 <- Hits %>%
  count(docname, hs_cat, name = "events") %>%
  pivot_wider(names_from = hs_cat, values_from = events, values_fill = 0) %>%
  left_join(meta_small, by = "docname") %>%
  filter(!is.na(year) & !is.na(country_org))

hs_year_country1 <- hs_df1 %>%
  group_by(year, country_org) %>%
  summarise(across(where(is.numeric), sum), .groups = "drop")

tok_df1 <- tibble(docname = docnames(corp),
                  tok_count = ntoken(toks)) %>%
  left_join(meta_small, by = "docname")

feature_cols1 <- setdiff(names(hs_year_country1), c("year", "country_org"))

tok_stats1 <- tok_df1 %>%
  filter(!is.na(year) & !is.na(country_org)) %>%
  group_by(year, country_org) %>%
  summarise(total_tokens = sum(tok_count), .groups = "drop")

hs_rates2 <- hs_year_country1 %>%
  left_join(tok_stats1, by = c("year", "country_org")) %>%
  rowwise() %>%
  mutate(HSAllCategories = sum(`Personal Security`, `Political Security`,
                               `Economic Security`, `Environmental Security`,
                               `Community Security`, `Food Security`, `Health Security`)) %>%
  ungroup() %>%
  mutate(across(all_of(feature_cols1),
                ~ (.x / total_tokens) * 1000, #needs to be 1k tokens
                .names = "{.col}_per1k")) %>%
  mutate(across(HSAllCategories,
                ~ (.x / total_tokens) * 1000, #adding it for plots
                .names = "{.col}_per1k"))

CreatePlots(PlotType = "timeseries", TheInputDF = hs_rates2) #returns timeseries plots grid as pdf

top_agendas <- Hits %>%
  left_join(meta_speeches, by = c("docname" = "filename")) %>%
  count(agenda_item_manual, hs_cat, sort = TRUE) %>%
  filter(!is.na(agenda_item_manual))

CreatePlots(PlotType = "Top Agenda", TheInputDF = top_agendas) #returns top agendas plot as pdf

hs_rates_per_meeting = Hits %>% left_join(meta_speeches, by = c("docname" = "filename"))

hs_rates_per_meeting_Adjusted = hs_rates_per_meeting %>%
  select(-c(3, 6, 8, 10)) 

tok_meeting <- tibble(docname = docnames(corp),
                      spv = docvars(corp, "spv"),
                      country_org = docvars(corp, "country_org"),
                      year = docvars(corp, "year"),
                      tokens = ntoken(toks)) %>%
  group_by(spv, country_org, year) %>%
  summarise(tokens = sum(tokens), .groups = "drop")

hs_meeting <- Hits %>%
  left_join(meta_small, by = "docname") %>%
  group_by(spv, country_org, year) %>%
  summarise(events = n(), .groups = "drop")

rate_meeting <- hs_meeting %>%
  left_join(tok_meeting, by = c("spv", "country_org", "year")) %>%
  mutate(rate_per1k = (events / tokens) * 1000)

hs_rates_meeting <- rate_meeting %>%
  group_by(year, country_org) %>%
  summarise(mean_rate_per1k = mean(rate_per1k),
            meetings = n(), .groups = "drop")

write_csv(hs_rates_meeting, "hs_rates_per_meeting_adjusted.csv")

write_csv(hs_rates_per_meeting_Adjusted, 
          str_c("Raw_HS_mentions_Analysis_Adjusted.csv"))

write_csv(hs_rates2, 
          str_c("HSRatesPer1kPlot_Data.csv"))

plan(sequential)