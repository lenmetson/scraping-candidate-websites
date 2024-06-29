download_latest <- function(fresh_pull = FALSE){

    date <- substr(Sys.time(), 1, 10)

    if (fresh_pull == TRUE){
        

        download.file(
            "https://candidates.democracyclub.org.uk/data/export_csv/?election_date=&ballot_paper_id=&election_id=parl.2024-07-04&party_id=&cancelled=&extra_fields=homepage_url&extra_fields=blog_url&extra_fields=party_ppc_page_url&extra_fields=wikipedia_url&extra_fields=other_url&extra_fields=gender&extra_fields=birth_date&format=csv", 
             paste0("data/", date, "_websites.csv"))

        websites <- read.csv(paste0("data/", date, "_websites.csv"))

    } else {
       if(file.exists(paste0("data/", date, "_websites.csv")) == TRUE){
        
        websites <- read.csv(paste0("data/", date, "_websites.csv"))

       } else {
        download.file(
            "https://candidates.democracyclub.org.uk/data/export_csv/?election_date=&ballot_paper_id=&election_id=parl.2024-07-04&party_id=&cancelled=&extra_fields=homepage_url&extra_fields=blog_url&extra_fields=party_ppc_page_url&extra_fields=wikipedia_url&extra_fields=other_url&extra_fields=gender&extra_fields=birth_date&format=csv", 
             paste0("data/", date, "_websites.csv"))

        websites <- read.csv(paste0("data/", date, "_websites.csv"))

       }
    }


return(websites)

}


websites <- download_latest(fresh_pull = TRUE) |>
    dplyr::mutate(
        has_page = ifelse(homepage_url != "", TRUE, FALSE)
    )

table(websites$has_page)


websites2 <- websites |>
    dplyr::filter(party_name == "Conservative and Unionist Party" | party_name == "Labour Party") |>
    dplyr::filter(homepage_url == "") |>
    dplyr::select(
        person_id, 
        party_name, 
        person_name, 
        homepage_url, party_ppc_page_url, wikipedia_url, has_page)  |>
    dplyr::mutate(checked = c())

table(websites2$has_page)
mean(websites2$has_page)



websites3 <- websites |>
    dplyr::filter(party_name == "Liberal Democrats" | party_name == "Green Party" | party_name == "Reform UK" | party_name == "Scottish National Party (SNP)") |>
    dplyr::filter(homepage_url == "") |>
    dplyr::select(
        person_id, 
        party_name, 
        person_name, 
        homepage_url, party_ppc_page_url, wikipedia_url, has_page)

set.seed(2029)
websites3 <- websites3 |>
    dplyr::sample_n(nrow(websites3))

write.csv(websites3, "websites_to_add3.csv")


table(websites_libdems$has_page)

websites_libdems <- websites |>
    dplyr::filter(party_name == "Liberal Democrats") |>
    # dplyr::filter(homepage_url == "") |>
    dplyr::select(
        person_id, 
        party_name, 
        person_name, 
        homepage_url, party_ppc_page_url, wikipedia_url, has_page)

table(websites_libdems$has_page)

table(websites$has_page)
# FALSE  TRUE 
#  3186  1332 

# FALSE  TRUE 24-6-8 
#  3135  1383 


mean(websites$has_page)
# 0.2948207 
# 0.3129703 24-6-8 18:47 
# 0.3253599 24-6-10 15:24s



set.seed(1032)
websites2 <- websites2 |>
    dplyr::sample_n(nrow(websites2))

write.csv(websites2, "websites_to_add.csv")

# table(websites$party_name, websites$has_page)
# table(websites$party_name)




