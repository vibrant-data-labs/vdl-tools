#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan  4 15:31:09 2023

@author: ericberlow
"""
# TODO: vdl-internal-tools
import vdl_tools.scrape_enrich.process_images as images # process and store logos
import pandas as pd
import pathlib as pl


## logo image director
wd = pl.Path.joinpath(pl.Path(__file__).parent.resolve(), '..')
logos = wd/"data"/"logos"
vdl_partners = logos/"VDL_Partner_Logos"


partner_logos_source = vdl_partners/"partner_logos_metadata.xlsx"
partner_logos_processed = vdl_partners/"partner_logos_processed.xlsx"

# READ ORIGINAL SOURCE URLS

df_logos_source = pd.read_excel(partner_logos_source)


#### PROCESS IMAGES
df_logos_processed = images.add_profile_images(df_logos_source,
                                                      partner_logos_processed,  # name of file to store image urls
                                                      vdl_partners/"partner_logos_errors.xlsx", # name of file to write image processing errors
                                                      idCol='id',  # col for merging
                                                      images_meta_path=str(vdl_partners), # local folder to hold image metadata files
                                                      nameCol='id',  # use for image filename
                                                      imageURL='source_url',  # image source url
                                                      image_directory = str(vdl_partners) + "/image_files", # local directory to store image files
                                                      bucket= "vdl-sponsor-logos",  # s3 bucket to store images
                                                      grayscale=False,  # convert to BW image
                                                      loadExisting=False)


#### CLEAN FINAL PROCESSED LOGOS WITH METADTA
df_logos_processed = df_logos_processed[['id', 'Image_URL']]
df_logos_processed.columns=['id', 'icon_url']
# get image title and org url
df_logos_meta = df_logos_source[['id', 'link_title', 'link_url', 'alt_link_url']]
# merge with processed image url
df_logos_processed = df_logos_processed.merge(df_logos_meta, on='id')

print("writing pricessed logos with metadata")
df_logos_processed.to_excel(partner_logos_processed, index=False)

