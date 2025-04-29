#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 12 10:31:59 2021

@author: ericberlow
"""


import pathlib as pl
import vdl_tools.shared_tools.common_functions as cf
import pandas as pd
import vdl_tools.scrape_enrich.params as p
import glob as glob
from url_normalize import url_normalize


def write_excel_no_hyper(df, outname):
    # write to excel without converting strings to hyperlinks
    writer = pd.ExcelWriter(
        outname,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )
    df.to_excel(writer, index=False, encoding="utf-8-sig")
    writer.close()


def read_combine_files(directory_path):
    # read in list of files pathnames from folder
    # return concatenated file
    print("\nreading files from %s" % directory_path)
    fileNames = glob.glob(directory_path)  # get list of filenames from directory

    df_list = []  # empty list to hold files
    for filename in fileNames:
        df_ = pd.read_csv(filename)
        search_term = filename.split(" - ")[1]
        df_["search_term"] = search_term
        df_list.append(df_)
    df = pd.concat(df_list)
    # aggreagate multiple search hits into search term list
    df_kwd = (
        df.groupby(["Organization Name URL"])["search_term"].agg(list).reset_index()
    )
    df_kwd["search_term"] = df_kwd["search_term"].apply(
        lambda x: "|".join(list(set(x)))
    )  # string of unique search terms
    df.drop(
        ["search_term"], axis=1, inplace=True
    )  # drop for merging with aggregated search list
    df = df.merge(df_kwd, on="Organization Name URL")
    df.drop_duplicates(inplace=True)  # remove duplicate entries for summarizing.
    return df


"""
def sum_funding_by_company(df, groupVars, tagCols, pickOneCols, yrCols, sumCols):
    # summarzie funding by group
    # pickOneCols - select first answer
    # tagCols - join as pipe-separated list of tags
    # sumCols - summarize numeric
    print("\nSummarizing Funding by Org")
    # clean text column missing values
    df[tagCols] = df[tagCols].fillna('').astype(str)
    #
    # build aggregation operations
    agg_data = {col: (lambda x: '|'.join(x)) for col in tagCols} # join as 'tags' separated by pipe
    agg_data.update({col: 'first' for col in pickOneCols})
    agg_data.update({col: 'sum' for col in sumCols})
    agg_data.update({col: 'max' for col in yrCols})
    # group and aggregate
    df_byCompany = df.groupby(groupVars).agg(agg_data).reset_index()
    # clean tags
    df_byCompany[tagCols] = cf.clean_tag_cols(df_byCompany, tagCols, delimeter="|")
    return df_byCompany
"""


def process_funding_rounds(fundingPath):  # =str(p.cb_funding_searches)):
    # read and combine multiple search result files from folder
    # filneame format = "projectname - search-term - suffix.csv"  - for search-term extraction
    # fundingPath = folder that has all the funding round search results
    df_funding = read_combine_files(fundingPath)
    df_funding["Announced Date"] = pd.to_datetime(
        df_funding["Announced Date"], errors="coerce"
    )
    df_funding["Funding_Year"] = df_funding["Announced Date"].dt.year
    df_funding["Funding_Years"] = df_funding["Funding_Year"].astype(str)
    df_funding["Raised_$"] = df_funding["Money Raised Currency (in USD)"]
    return df_funding


def summarize_funding_rounds_by_company(df_f, minYear=None):
    # summarize funding rounds by organization
    # df_f = cleaned file of funding rounds results
    # minYear =  filter by most recent year funded
    print("\nSummarizing Funding by Org")

    if minYear is not None:
        # filter by year
        print("Summarizing funding since %s (including that year)" % str(minYear))
        df_f = df_f[(df_f["Funding_Year"] >= minYear)]
        df_f = df_f.reset_index(drop=True)

    # add cols for most recent funding type
    df_f["Last_Funding_Type"] = df_f["Funding Type"]

    group = ["Organization Name URL"]
    tagCols = ["Lead Investors", "Investor Names", "Funding Type", "Funding_Years"]
    pickFirstCols = ["Last_Funding_Type"]
    yrCols = ["Funding_Year"]
    sumCols = ["Raised_$"]

    # sort by company and year (most recent year first)
    df_f.sort_values(group + yrCols, ascending=[True, False])
    # clean text column missing values
    df_f[tagCols] = df_f[tagCols].fillna("").astype(str)

    # build aggregation operations
    agg_data = {
        col: (lambda x: "|".join(x)) for col in tagCols
    }  # join as 'tags' separated by pipe
    agg_data.update({col: "first" for col in pickFirstCols})
    agg_data.update({col: "sum" for col in sumCols})
    agg_data.update({col: "max" for col in yrCols})
    # group and aggregate
    df_f_byCompany = df_f.groupby(group).agg(agg_data).reset_index()
    # clean tags
    df_f_byCompany[tagCols] = cf.clean_tag_cols(df_f_byCompany, tagCols, delimeter="|")
    # clean empty
    df_f_byCompany["Raised_$"].fillna(0, inplace=True)

    # rename columns
    df_f_byCompany.rename(
        columns={
            "Organization Name URL": "Crunchbase_URL",
            "Raised_$": "Total_Raised_$",
            "Funding_Year": "Last_Raised_Year",
            "Funding Type": "Funding Types",
            "Investor Names": "Investors",
        },
        inplace=True,
    )
    return df_f_byCompany


def process_company_searches(companiesPath):  # = (str(p.cb_company_searches)) ):
    # read and combine multiple search result files from folder
    # filneame format = "projectname - search-term - suffix.csv"  - for search-term extraction
    # companiesPath = folder that has all the company  search results

    df_companies = read_combine_files(companiesPath)
    # drop duplicate results
    df_companies.drop_duplicates(subset=["Organization Name URL"], inplace=True)
    # normalize urls
    df_companies[["Website", "LinkedIn"]] = df_companies[
        ["Website", "LinkedIn"]
    ].fillna("")
    df_companies["Website"] = df_companies["Website"].apply(
        lambda x: url_normalize(x)
    )  # automated url normalizer
    df_companies["LinkedIn"] = cf.normalize_linkedin_urls(
        df_companies, "LinkedIn"
    )  # clean linkedin urls

    # process datetime
    df_companies["Last Funding Date"] = pd.to_datetime(
        df_companies["Last Funding Date"], errors="coerce"
    )
    df_companies["Founded Date"] = pd.to_datetime(
        df_companies["Founded Date"], errors="coerce"
    )
    df_companies["Year_Last_Funded"] = df_companies["Last Funding Date"].dt.year
    df_companies["Founded_Year"] = df_companies["Founded Date"].dt.year
    # convert Industries, Founders to tags
    for col in ["Industries", "Industry Groups"]:
        df_companies[col].fillna("", inplace=True)
        df_companies[col] = df_companies[col].str.lower().str.replace(", ", "|")
    df_companies["Founders"] = df_companies["Founders"].str.replace(", ", "|")

    # simplify funding status names
    # map funding status renaming
    statusRename = {
        "Early Stage Venture": "Early Venture",
        "Late Stage Venture": "Late Venture",
    }
    for s1, s2 in statusRename.items():
        df_companies["Funding Status"] = df_companies["Funding Status"].str.replace(
            s1, s2
        )

    # combine descriptions
    df_companies["Description"] = cf.join_strings_no_missing(
        df_companies, ["Description", "Full Description"], delim=" // "
    )
    df_companies.rename(
        columns={"Organization Name URL": "Crunchbase_URL"}, inplace=True
    )
    # rename columns
    df_companies.rename(
        columns={
            "Organization Name URL": "Crunchbase_URL",
            "Total Funding Amount Currency (in USD)": "Total_Funding_$",
        },
        inplace=True,
    )
    return df_companies


def process_crunchbase(
    fundingPath,  # =str(p.cb_funding_searches),
    companiesPath,  # =(str(p.cb_company_searches)),
    lastYear=None,  # option to filter funding rounds by most recent year funded
    cb_outfile=None,  # pathname of processed file
):
    # read, combine, process all csv results from company searches - filneame = "projectname - search-term - suffix.csv"
    # read, combine, process all results from funding rounds searches - fname = "projectname - search-term - suffix.csv"
    # summarize funding rounds by company
    # merge funding data to company data
    print("\nPROCESSING RAW CRUNCHBASE DATA")
    # ## PROCESS FUNDING SEARCHES
    df_funding = process_funding_rounds(fundingPath=fundingPath)
    df_funding_byCompany = summarize_funding_rounds_by_company(
        df_funding, minYear=lastYear
    )

    # ## PROCESS COMPANY SEARCHES
    df_companies = process_company_searches(companiesPath=companiesPath)

    # ## MERGE FUNDING DATA WITH COMPANY DATA
    print("\nAdding summarized funding round data to company data")
    df_companies_funding = df_companies.merge(
        df_funding_byCompany, on="Crunchbase_URL", how="left"
    )
    # fill missing investors, convert list to tags
    df_companies_funding["Investors"] = df_companies_funding["Investors"].fillna(
        df_companies_funding["Top 5 Investors"]
    )
    df_companies_funding["Investors"] = df_companies_funding["Investors"].str.replace(
        ", ", "|"
    )
    df_companies_funding["Investors"].fillna("", inplace=True)
    df_companies_funding["Investors"] = cf.clean_tag_cols(
        df_companies_funding, ["Investors"]
    )
    # fill mising raised with company total funding
    df_companies_funding["Total_Raised_$"] = df_companies_funding.apply(
        lambda x: x["Total_Funding_$"]
        if x["Total_Raised_$"] == 0
        else x["Total_Raised_$"],
        axis=1,
    )
    # add Public Company if IPO
    df_companies_funding["Funding Types"].fillna("", inplace=True)
    df_companies_funding["Company Type"] = df_companies_funding.apply(
        lambda x: "Public Company"
        if "IPO" in x["Funding Types"]
        else x["Company Type"],
        axis=1,
    )
    # add id
    df_companies_funding["id"] = df_companies_funding["Crunchbase_URL"]
    # CLEAN FINAL COLUMNS
    df_companies_funding.rename(
        columns={
            "Estimated Revenue Range": "Estimated_Revenue",
            "Number of Employees": "n_Employees",  # ordinal categories
            "Number of Investors": "n_Investors",
            "Number of Funding Rounds": "n_Funding_Rounds",
        },
        inplace=True,
    )

    keepCols = [
        "Crunchbase_URL",
        "Organization Name",
        "Headquarters Location",
        "Headquarters Regions",
        "Description",
        # 'Full Description',
        "Founders",
        "Website",
        "LinkedIn",
        "Funding Status",
        "Funding Types",
        "Last_Funding_Type",
        "n_Funding_Rounds",
        "Estimated_Revenue",
        "n_Employees",
        "Company Type",
        "Industries",
        "Industry Groups",
        "Acquired by",
        "Acquired by URL",
        "search_term",
        "Investors",
        "Year_Last_Funded",  # from companies data
        "Founded_Year",  # from copmanies data
        "Funding_Years",  # all years funded from funding rounds data
        "Total_Funding_$",  # from copmanies data
        "Total_Raised_$",  # from funding ronds > summary for time interval if filtered
        "n_Investors",
        "id"
        # 'Total_Raised_$', # from funding rounds data
        # 'Last_Raised_Year', # from funding rounds data
    ]

    df_companies_funding = df_companies_funding[keepCols]
    df_companies_funding.sort_values(
        ["Year_Last_Funded"], ascending=False, inplace=True
    )
    df_companies_funding = df_companies_funding.reset_index(drop=True)

    if cb_outfile is not None:
        # ## WRITE FILE
        write_excel_no_hyper(df_companies_funding, cb_outfile)

    return df_companies_funding, df_companies, df_funding_byCompany


if __name__ == "__main__":
    wd = pl.Path.cwd()
    df_companies_funding, df_companies, df_funding_by_company = process_crunchbase(
        fundingPath=str(p.cb_funding_searches(wd)),
        companiesPath=(str(p.cb_company_searches(wd))),
        lastYear=None,  # option to filter funding rounds by most recent year funded
        cb_outfile=None,  # pathname of processed file
    )
    # ## FILTERING OPTIONS

    # ## FILTER OUT THOSE WHERE TOTAL FUNDING = 0
    # print('\nRemoving Companies with 0 Total Funding')
    # w_funding = df_companies_wfunding['Total_Funding_$'] > 0
    # df_companies_wfunding = df_companies_wfunding[w_funding]

    # companies funded or founded in the past 5 yrs
    # last_5yrs = (df_companies_wfunding['Year_Last_Funded'] >2016) | ((df_companies_wfunding['Founded_Year'] >2016) & (df_companies_wfunding['Founded_Year'] < df_companies_wfunding['Year_Last_Funded']))
    # df_companies_wfunding = df_companies_wfunding[last_5yrs]

    # ## WRITE FILE
    write_excel_no_hyper(df_companies_funding, p.cb_companies_cleaned(wd))
