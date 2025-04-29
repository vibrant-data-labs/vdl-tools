#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 26 18:31:48 2022

@author: ericberlow
"""
import pathlib as pl

import pandas as pd

from vdl_tools.download_process_images.imagetools import (
    download_images_df,
    get_image_name,
    process_images,
    upload_images_df,
    S3_CLIENT
)
from vdl_tools.shared_tools.s3_tools import list_bucket_contents
import vdl_tools.shared_tools.common_functions as cf  # from common directory: commonly used function
from vdl_tools.shared_tools.tools.logger import logger



#################################################################################################
# PROCESS IMAGES - functions to get logos from url, process them, and store them in S3 bucket
#################################################################################################


def process_profile_images(
    df,
    id_col,
    name_col,
    image_url,
    images_name,
    errors_name,
    image_directory,
    bucket,
    grayscale=True,
):
    """
    Process profile images.

    Download images to local folder
    process the images
    upload to S3 and add stable S3 image url to dataframe
    ----------
     df : dataframe must have unique id or name or merging, and raw image url
     id_col : folumn with unique id for merging with other data
     name_col: string for naming the filename and the s3 url
     image_url :  column raw url
     image directory: local directory to store images
     bucket: s3 bucket name to store images and get url

    Returns dataframe with logo url's and documented errors
    """
    # df = df.head(100) #sample for testing
    df_images = df[[id_col, name_col, image_url]].copy()  # subset columns with image name and url
    df_images.columns = [
        id_col,
        "name",
        "image_url",
    ]  # rename columns for processing - must have these names
    df_images["filename"] = df_images[
        "name"
    ]  # must have 'filename' column (i think it doesn't use it - need to fix)

    # download images to local directory and update dataframe with filename column
    df_images, df_images_errors = download_images_df(
        df_images,
        image_dir=image_directory,
        as_png=True  # convert all to png if possible
    )

    # write errors to an excel file
    cf.write_excel_no_hyper(df_images_errors, errors_name)

    # process all images in the local directory (e.g. resize, convert to grey)
    image_names = [x.split("/")[-1] for x in df_images["filename"]]
    process_images(
        image_names,
        image_dir=image_directory,
        resize=True,
        width=200,
        height=200,
        grayscale=grayscale,
        padding=False,
        padding_width=100,
        padding_height=100,
    )

    # upload images in the local directory to s3 and update dataframe with s3 endpoint
    df_images = upload_images_df(df_images, image_dir=image_directory, s3_bucket=bucket)

    # add original column names and s3 url cleaned of errors
    df_images = df_images.reset_index(drop=True)
    df_images.rename(
        columns={"filename": "image_filename", "image_url": "image_source_url"},
        inplace=True,
    )
    # replace error messages with empty string
    df_images["Image_URL"] = df_images["s3_url"].apply(
        lambda x: "" if "ERROR" in x else x
    )
    # flagg s3 error messages - (no .png file found because svg not converted)
    df_images["image_error"] = df_images["s3_url"].apply(
        lambda x: ("SVG Error") if "ERROR" in x else ""
    )

    def image_mapping(x):
        if 'image_filename' in x:
            return  x.image_filename if x.image_filename in ["other url error", "no url", "url broken"] else x.image_error
        
        return 'url broken'

    df_images["image_error"] = df_images.apply(
        image_mapping,
        axis=1,
    )

    if 'image_filename' in df_images.columns:
        df_images["image_filename"] = df_images["image_filename"].apply(
            lambda x: "" if x in ["url broken", "other url error", "no url"] else x
        )  # clean out errors
    else:
        df_images["image_filename"] = 'no url'
    # cleaned dataset final columns to keep
    df_images = df_images[
        [id_col, "Image_URL", "image_error", "image_filename", "image_source_url"]
    ]
    # print("writing processed image url file")
    # cf.write_excel_no_hyper(df_images, images_name)
    return df_images


def add_profile_images(
    df,
    images_name,  # name of file to save successfully processed images
    errors_name,  # name of file to write image processing errors
    id_col="id",
    image_directory=None,  # if none - default directory to store image files = "data/images/image_files"
    images_meta_path=None,  # if none - default directory to store image metdata = "data/images"
    name_col="profile_name",  # image name
    image_url="image_url",  # original image url to be processed
    bucket="openmappr-images",  # s3 bucket to store images
    grayscale=False,
    load_existing=False,
):
    # add processed logo url's to recipient metadata file
    # get logos if processed file exists, otherwise scrape/process from raw url
    # df must have [id_col, image_url] raw logo url's scraped from the web
    # images_name = pathname of processed images file

    if image_directory is None:
        image_directory = str(pl.Path.cwd() / "data" / "images" / "image_files")
    if images_meta_path is None:
        images_meta_path = str(pl.Path.cwd() / "data" / "images")
    logger.info("\nAdding Logos")
    if load_existing:
        existing_s3_files = set(list_bucket_contents(S3_CLIENT, bucket))
        df['image_filename'] = df[name_col].apply(lambda x: f"{get_image_name(x)}.png")

        has_previous_image = df['image_filename'].apply(lambda x: x in existing_s3_files)
        df_images_previous = df[has_previous_image][[id_col, "image_filename", image_url]].copy()
        df_images_previous["Image_URL"] = df_images_previous["image_filename"].apply(
            lambda x: f"https://{bucket}.s3.amazonaws.com/{x}"
        )

        df_images_previous.rename(columns={"image_filename": "image_source_url"}, inplace=True)
        df_images_previous["image_error"] = ""
        df_images_previous["image_filename"] = "Previously Uploaded"

        df_images_previous = df_images_previous[[id_col, "Image_URL", "image_error",
                                                 "image_filename", "image_source_url"]]
        df_needs_image = df[~has_previous_image]
        logger.info("Found %s existing images", len(df_images_previous))
        logger.info("Processing %s orgs with unfound images", len(df_needs_image))

        df.drop("image_filename", axis=1, inplace=True)
        df_images_new = process_profile_images(
            df_needs_image,
            id_col,
            name_col,
            image_url,
            images_name,
            errors_name,
            image_directory,
            bucket,
            grayscale=grayscale,
        )

        df_images = pd.concat([df_images_previous, df_images_new], axis=0)
    else:  # scrape logos from url, process, and upload to s3
        print("scraping and processing logo images")
        df_images = process_profile_images(
            df,
            id_col,
            name_col,
            image_url,
            images_name,
            errors_name,
            image_directory,
            bucket,
            grayscale=grayscale,
        )

    # merge logo data to Recipient summary file
    df.drop([image_url], axis=1, inplace=True)  # clean original url column
    df_w_images = df.merge(df_images, on=id_col, how="left")

    # clean columns
    df_w_images.drop([], axis=1, inplace=True)

    # flag errors and write error file
    df_image_errors = df_w_images[
        [id_col, "Image_URL", "image_error", "image_filename", "image_source_url"]
    ]
    df_image_errors = df_image_errors[df_image_errors["Image_URL"] == ""]
    cf.write_excel_no_hyper(
        df_image_errors, errors_name
    )

    return df_w_images
