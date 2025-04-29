# -*- coding: utf-8 -*-
"""
process Candid grants data - funded organizations, funders, people, prograams
"""

import vdl_tools.shared_tools.common_functions as cf
import pandas as pd


def write_excel_no_hyper(df, outname):
    # write to excel without converting strings to hyperlinks
    writer = pd.ExcelWriter(
        outname,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )
    df.to_excel(writer, index=False, encoding="utf-8-sig")
    writer.close()


def process_candid_main_org_data(df_main):
    ### PROCESS BASIC COMPANY DATA
    print("\nProcessing/cleaning main org data")
    df_main = df_main.reset_index(drop=True)
    # merge address
    address_cols = ["address_line1", "address_line2", "city", "state_code", "zip_code"]
    df_main[address_cols] = df_main[address_cols].fillna("")
    df_main["hq_address"] = cf.join_strings_no_missing(
        df_main, address_cols, delim=", "
    )

    # convert pcs codes to sector tags
    pcs_cols = ["pcs_subject", "pcs_population"]
    for col in pcs_cols:
        df_main[col] = df_main[col].fillna("").str.lower()
        df_main[col] = df_main[col].apply(lambda x: x.split("; "))  # convert to list
        df_main[col] = df_main[col].apply(
            lambda x: [tag.split(" ")[1:] for tag in x]
        )  # remove code from each item in the list
        df_main[col] = df_main[col].apply(
            lambda x: [" ".join(wordlist) for wordlist in x]
        )  # re-join words in each item
        df_main[col] = df_main[col].apply(
            lambda x: [tag for tag in x if "unknown" not in x]
        )  # remove uknown classifications
        df_main[col] = df_main[col].apply(
            lambda x: "|".join(x)
        )  # join into pipe-separated tags

    # convert ntee codes to industry
    df_main["industry_cd"] = df_main["ntee_description"].fillna("").str.lower()
    df_main["industry_cd"] = df_main["industry_cd"].str.replace(
        " N.E.C.", "", regex=True
    )  # remove NEC

    # normalize website urls
    df_main["website"] = df_main["website"].fillna("")
    df_main["website"] = df_main["website"].apply(
        lambda x: x.lower().split("://")[-1]
    )  # remove prefix
    df_main["website"] = df_main["website"].apply(
        lambda x: "https://" + x if x != "" else x
    )  # add standard prefix
    # df_main['website'] = df_main['website'].apply(lambda x: url_normalize(x))
    # clean linkedin urls
    li_url = "socialLinkedIn"
    df_main[li_url] = df_main[li_url].fillna("")
    df_main[li_url] = df_main[li_url].apply(
        lambda x: x.split("?")[0]
    )  # remove long suffix
    df_main[li_url] = (
        df_main[li_url].str.lower().str.strip()
    )  # make all lower case, remove any trailing space
    df_main[li_url] = df_main[li_url].str.replace(
        "admin/", "", regex=True
    )  # remove admin suffix
    df_main[li_url] = df_main[li_url].apply(
        lambda x: ""
        if x == ""
        else x[:-1]
        if x[-1] == "/"  # remove trailing "/"
        else x
    )
    df_main[li_url] = df_main[li_url].apply(lambda x: x.split("/")[-1])  # get li id
    df_main[li_url] = df_main[li_url].apply(
        lambda x: "https://www.linkedin.com/company/" + x if x != "" else x
    )

    # add company type and funding type
    df_main["Org Type"] = "Non Profit"
    df_main["Funding Types"] = "Grant"
    df_main["Last_Funding_Type"] = "Grant"
    df_main["Funding Stage"] = "Philanthropy"
    df_main["id"] = df_main["ein"]
    #

    # combine mission cols
    missionCols = [x for x in df_main.columns.tolist() if "mission" in x]
    df_main["mission"] = cf.join_strings_no_missing(df_main, missionCols, delim=" // ")
    df_main["mission"] = cf.clean_tag_cols(df_main, ["mission"], delimeter=" // ")

    # add # employee categories
    df_main["total_employees"] = pd.to_numeric(
        df_main["total_employees"], errors="coerce"
    )
    df_main["n_Employees"] = df_main["total_employees"].apply(
        lambda x: "1-10"
        if ((x > 0) and (x <= 10))
        else "11-50"
        if ((x > 10) and (x <= 50))
        else "51-100"
        if ((x > 50) and (x <= 100))
        else "101-250"
        if ((x > 100) and (x <= 250))
        else "251-500"
        if ((x > 250) and (x <= 500))
        else "501-1000"
        if ((x > 500) and (x <= 1000))
        else "1001-5000"
        if ((x > 1000) and (x <= 5000))
        else "5001-10000"
        if ((x > 5000) and (x <= 10000))
        else "10001+"
        if (x > 10000)
        else None
    )

    # clean cols
    maincols = [
        "ein",
        "organization_name",
        "mission",
        "pcs_subject",
        "pcs_population",
        "industry_cd",
        "Org Type",
        "total_employees",  # number
        "n_Employees",  # ordinal categories
        "total_funding",
        "Funding Types",
        "Last_Funding_Type",
        "Funding Stage",
        "most_recent_funding_year",
        "hq_address",
        "website",
        "socialLinkedIn",
        "profile_link",
        "logo_url",
        "id",
    ]

    rename_main = {
        "organization_name": "Organization Name",
        "website": "Website",
        "socialLinkedIn": "LinkedIn",
        "pcs_subject": "sector_cd",
        "pcs_population": "population_cd",
        "logo_url": "logo",
        "profile_link": "Candid_URL",
        "total_funding": "Total_Funding_$",
        "total_employees": "Employees_cd",
        "most_recent_funding_year": "Year_Last_Funded",
    }

    df_main = df_main[maincols]
    df_main.rename(columns=rename_main, inplace=True)

    return df_main


def process_add_candid_funding(df_funders, df_main):
    ### PROCESS AND ADD FUNDING DATA
    print("\nAdding funders by Org")
    # aggregate funders into list
    df_funding_byOrg = (
        df_funders.groupby("recip_ein")["gm_name"].agg(list).reset_index()
    )
    df_funding_byOrg.columns = ["ein", "Funders"]
    df_funding_counts = (
        df_funders.groupby("recip_ein")["gm_name"].agg("size").reset_index()
    )
    df_funding_counts.columns = ["ein", "n_Grants"]
    df_funding_byOrg = df_funding_byOrg.merge(df_funding_counts, on="ein")
    # tally number of funders and convert list to pipe-separated tags
    df_funding_byOrg["n_Funders"] = df_funding_byOrg["Funders"].apply(lambda x: len(x))
    df_funding_byOrg["Funders"] = df_funding_byOrg["Funders"].apply(
        lambda x: "|".join(x)
    )  # create tags
    # add funder info to main org data
    df_main_wFunders = df_main.merge(df_funding_byOrg, on="ein", how="left")
    return df_main_wFunders


def process_add_candid_people(df_ppl, df_main):
    ### PROCESS AND ADD BOARD MEMEBERS, EXECUTIVES, FOUNDERS
    print("\nAdding people by Org")
    df_ppl["title"] = df_ppl["title"].str.lower()
    df_ppl["title"] = df_ppl["title"].fillna("")
    df_ppl["name"] = df_ppl["name"].str.title()
    df_ppl["name"] = df_ppl["name"].fillna("")
    # subset and aggregate board members
    board_list = ["board", "chair", "chairman", "trustee"]
    # flag True if any of the strings in the list are in the title
    is_on_board = df_ppl["title"].apply(lambda x: any(term in x for term in board_list))
    df_ppl_boards = df_ppl[is_on_board].reset_index(drop=True)
    df_ppl_boards["name"].fillna("", inplace=True)
    df_ppl_boards_byOrg = df_ppl_boards.groupby("ein")["name"].agg(list).reset_index()
    df_ppl_boards_byOrg.columns = ["ein", "Board"]
    df_ppl_boards_byOrg["Board"] = df_ppl_boards_byOrg["Board"].apply(
        lambda x: "|".join(x) if x else x
    )  # create tags

    # subset and aggreagate executives
    exec_list = [
        "ceo",
        "chief",
        "cfo",
        "cmo",
        "coo",
        "cso",
        "cio",
        "cpo",
        "cvo",
        "president",
        "pres",
        "vice president",
        "vice-president",
        "vp",
        "evp",
        "svp",
        "v.p.",
        "executive director",
        "ed",
        "e.d.",
        "exec dir",
        "exec director",
        "founder",
        "founding",
    ]
    # flag True if any of the strings in the list are in the title
    is_exec = df_ppl["title"].apply(lambda x: any(term in x for term in exec_list))
    df_ppl_execs = df_ppl[is_exec].reset_index(drop=True)

    df_ppl_execs_byOrg = df_ppl_execs.groupby("ein")["name"].agg(list).reset_index()
    df_ppl_execs_byOrg.columns = ["ein", "Executives"]
    df_ppl_execs_byOrg["Executives"] = df_ppl_execs_byOrg["Executives"].apply(
        lambda x: "|".join(x)
    )  # create tags

    # subset and aggreagate founders
    founder_list = ["founder", "founding"]
    # flag True if any of the strings in the list are in the title
    is_founder = df_ppl["title"].apply(
        lambda x: any(term in x for term in founder_list)
    )
    df_ppl_founders = df_ppl[is_founder].reset_index(drop=True)
    df_ppl_founders_byOrg = (
        df_ppl_founders.groupby("ein")["name"].agg(list).reset_index()
    )
    df_ppl_founders_byOrg.columns = ["ein", "Founders"]
    df_ppl_founders_byOrg["Founders"] = df_ppl_founders_byOrg["Founders"].apply(
        lambda x: "|".join(x)
    )  # create tags

    # add ppl tags to main org file
    df_main_w_board = df_main.merge(df_ppl_boards_byOrg, on="ein", how="left")
    df_main_w_board_execs = df_main_w_board.merge(
        df_ppl_execs_byOrg, on="ein", how="left"
    )
    df_main_w_board_execs_fndrs = df_main_w_board_execs.merge(
        df_ppl_founders_byOrg, on="ein", how="left"
    )

    return df_main_w_board_execs_fndrs


def process_add_program_descriptions(df_prog, df_main):
    # combine all the proram descriptions for each org
    # add them to the main org file
    print("\nAdding program descriptions by Org")
    df_prog.rename(columns={"EIN": "ein"}, inplace=True)

    # clean a text columsn missing values
    textcols = ["program_name", "program_description"]
    df_prog[textcols] = df_prog[textcols].fillna("")
    # merge program name with description
    df_prog["Description"] = cf.join_strings_no_missing(df_prog, textcols, delim=" // ")
    # build aggregation operations
    agg_data = {
        "Description": (lambda x: " // ".join(x))
    }  # join multiple grants into one description
    # group and aggregate
    df_prog_byOrg = df_prog.groupby("ein").agg(agg_data).reset_index()
    # clean tags
    df_prog_byOrg["Description"] = cf.clean_tag_cols(
        df_prog_byOrg, ["Description"], delimeter=" // "
    )
    # add program descriptions to main org file
    df_main_w_prog = df_main.merge(df_prog_byOrg, on="ein", how="left")
    # combine mission with description
    df_main_w_prog["Description"] = cf.join_strings_no_missing(
        df_main_w_prog, ["mission", "Description"], delim=" // "
    )
    return df_main_w_prog


def clean_990(text):
    # CONVERT ALL CAPS TO Sentence case.
    chunk_list = text.split(" // ")  # list of separate descriptions
    cleaned_chunk_list = []
    for chunk in chunk_list:
        chunk_sentences = chunk.split(". ")  # list of sentences within chunk
        # capitalize each sentence
        chunk_sentences = [sentence.capitalize() for sentence in chunk_sentences]
        # re combine all sentences in the chunk
        chunk_cleaned = ". ".join(chunk_sentences)
        cleaned_chunk_list.append(chunk_cleaned)
    text_cleaned = " // ".join(cleaned_chunk_list)
    return text_cleaned


def process_candid(
    cd_main,
    cd_funders,
    cd_personnel,
    cd_programs,
    cd_filings=None,
    outfile=None,
    filterFunded=False,
):
    """
    # process, summarize, combine all candid raw data
    cd_main: filename of main org data
    cd_funders: filename of funders for each org
    cd_personnel: filename of people for each org
    cd_programs: filename of program descriptions for each org/grant
    outfile: filename of processed final file to write
    """
    print("\nPROCESSING RAW CANDID DATA")
    # READ DATA

    """
    # excel format
    df_main_raw =  pd.read_excel(cd_main, engine='openpyxl')
    df_funders = pd.read_excel(cd_funders, engine='openpyxl')
    df_ppl = pd.read_excel(cd_personnel, engine='openpyxl')
    df_prog = pd.read_excel(cd_programs, engine='openpyxl')
    """
    # uft-16-le text file
    # if having trouble reading the files, try encoding="UTF-16 LE"
    # TODO: eliminate error_bad_lines variable and find a way to inspect the files and fix errors before reading them
    df_main_raw = pd.read_csv(cd_main, sep="|",  encoding="UTF-16", on_bad_lines='warn', dtype=str) 
    df_funders = pd.read_csv(cd_funders, sep="|", encoding="UTF-16",  on_bad_lines='warn', dtype=str)
    df_ppl = pd.read_csv(cd_personnel, sep="|",  encoding="UTF-16", on_bad_lines='warn', dtype=str)
    df_prog = pd.read_csv(cd_programs, sep="|", encoding="UTF-16", on_bad_lines='warn', dtype=str)

    # ## PROCESS BASIC COMPANY DATA
    df_main = process_candid_main_org_data(df_main_raw)

    # ## PROCESS AND ADD FUNDING DATA
    df_main_w_funders = process_add_candid_funding(df_funders, df_main)

    # ## PROCESS AND ADD PERSONNEL DATA
    df_main_w_funders_ppl = process_add_candid_people(df_ppl, df_main_w_funders)

    # ## PROCESS AND ADD PROGRAM DESCRIPTION DATA
    df_main_w_funders_ppl_prog = process_add_program_descriptions(
        df_prog, df_main_w_funders_ppl
    )

    # ## PROCESS AND ADD 990 FILINGS PROGRAM DATA ###
    if cd_filings is not None:
        print("\nAdding program descriptions from 990 filings")
        df_filing = pd.read_csv(cd_filings, sep="|",  encoding="UTF-16", on_bad_lines='warn', dtype=str)
        # df_filing = pd.read_excel(cd_filings, engine='openpyxl')
        df_filing.rename(
            columns={"program_description": "Description_990"}, inplace=True
        )
        df_filing.fillna("", inplace=True)
        # build aggregation operations
        agg_data = {
            "Description_990": (lambda x: " // ".join(x))
        }  # join multiple filings into one description
        # group and aggregate
        df_filing_byOrg = df_filing.groupby("ein").agg(agg_data).reset_index()
        # clean tags
        df_filing_byOrg["Description_990"] = cf.clean_tag_cols(
            df_filing_byOrg, ["Description_990"], delimeter=" // "
        )
        # add program descriptions to main org file
        df_main_w_funders_ppl_prog = df_main_w_funders_ppl_prog.merge(
            df_filing_byOrg, on="ein", how="left"
        )

    ### FILTERING OPTIONS
    if filterFunded:
        # only keep if total funding > 0
        df_main_w_funders_ppl_prog = df_main_w_funders_ppl_prog[
            df_main_w_funders_ppl_prog["Total_Funding_$"] > 0
        ]
        df_main_w_funders_ppl_prog = df_main_w_funders_ppl_prog.reset_index(drop=True)

    ## CLEAN FINAL COLUMNS
    keepCols = [
        "id",
        "Organization Name",
        # 'mission',
        "Description",
        "Description_990",
        "sector_cd",
        "population_cd",
        "industry_cd",
        "Org Type",
        "n_Employees",
        "Employees_cd",
        "Total_Funding_$",
        "Funding Stage",
    #    "Funding Status",
        "Funding Types",
        "Last_Funding_Type",
        "n_Grants",
        "Year_Last_Funded",
        "hq_address",
        "Website",
        "LinkedIn",
        "Candid_URL",
        "logo",
        "Funders",
        "n_Funders",
        "Founders",
        "Board",
        "Executives",
        "ein",
    ]

    df_main_w_funders_ppl_prog = df_main_w_funders_ppl_prog[keepCols]

    # sort by year
    df_candid = df_main_w_funders_ppl_prog.sort_values(
        ["Year_Last_Funded"], ascending=False
    )
    df_candid.reset_index(drop=True, inplace=True)
    if outfile is not None:
        ### WRITE FILE
        write_excel_no_hyper(df_candid, outfile)
    return df_candid


# if __name__ == '__main__':

# df_candid = process_candid(p.cd_main, p.cd_funders,  p.cd_personnel, p.cd_programs,  outfile=None)
